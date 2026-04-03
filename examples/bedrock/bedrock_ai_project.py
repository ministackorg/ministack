#!/usr/bin/env python3
"""
Mini projet IA/ML avec AWS Bedrock — fonctionne sur MiniStack et AWS.

Simule un pipeline RAG complet :
  1. Découverte des modèles disponibles
  2. Création d'un guardrail de sécurité
  3. Upload de documents dans S3
  4. Création d'une Knowledge Base + Data Source
  5. Ingestion des documents (S3 → pgvector)
  6. Recherche sémantique (Retrieve)
  7. RAG — Retrieve & Generate
  8. Conversation directe (Converse + InvokeModel)
  9. Comptage de tokens
  10. Test du guardrail (contenu autorisé + bloqué)
  11. Nettoyage

Usage:
    # Contre MiniStack (défaut)
    python examples/bedrock_ai_project.py

    # Contre AWS
    python examples/bedrock_ai_project.py --aws

    # Avec un modèle spécifique
    python examples/bedrock_ai_project.py --model eu.anthropic.claude-sonnet-4-6

Prérequis:
    pip install boto3
    docker compose up -d   (pour MiniStack)
"""

import argparse
import io
import json
import sys
import time
import uuid

import boto3
from botocore.config import Config

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

MINISTACK_ENDPOINT = "http://localhost:4566"
DEFAULT_REGION = "us-east-1"
DEFAULT_MODEL = "anthropic.claude-3-sonnet-20240229-v1:0"

# Documents de la "base de connaissances" de notre projet fictif
KNOWLEDGE_DOCS = {
    "docs/architecture.txt": (
        "Notre application utilise une architecture microservices avec 3 composants principaux : "
        "un API Gateway (FastAPI), un service de traitement (Celery + Redis), et une base de données "
        "PostgreSQL avec pgvector pour la recherche sémantique. Le tout est déployé sur AWS ECS Fargate."
    ),
    "docs/deployment.txt": (
        "Le déploiement se fait via Terraform. Les environnements dev/staging/prod sont séparés par "
        "des comptes AWS distincts. Le CI/CD utilise GitHub Actions avec des étapes de lint, test, "
        "build Docker, push ECR, et deploy ECS. Le rollback est automatique si le health check échoue."
    ),
    "docs/security.txt": (
        "La sécurité est assurée par AWS WAF sur l'API Gateway, des guardrails Bedrock pour filtrer "
        "les contenus sensibles (PII, mots de passe), et le chiffrement KMS pour les secrets. "
        "L'authentification utilise Cognito avec MFA obligatoire pour les admins."
    ),
    "docs/monitoring.txt": (
        "Le monitoring repose sur CloudWatch Metrics et Logs, avec des alarmes sur la latence P99, "
        "le taux d'erreur 5xx, et l'utilisation CPU/mémoire des containers ECS. "
        "Un dashboard Grafana agrège les métriques métier (requêtes/sec, coût LLM, tokens consommés)."
    ),
    "docs/llm_usage.txt": (
        "L'application utilise Amazon Bedrock pour l'inférence LLM. Le modèle principal est "
        "Claude 3 Sonnet pour la génération de réponses. Les embeddings sont calculés avec "
        "Amazon Titan Embed Text v1. Un système de cache Redis réduit les appels API de 40%."
    ),
}


def make_client(service, endpoint=None, region=DEFAULT_REGION):
    """Créer un client boto3, configuré pour MiniStack ou AWS."""
    kwargs = dict(
        region_name=region,
        config=Config(
            retries={"max_attempts": 2},
            read_timeout=120,
            connect_timeout=10,
        ),
    )
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    return boto3.client(service, **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# Étapes du projet
# ─────────────────────────────────────────────────────────────────────────────

def step_1_discover_models(bedrock):
    """Étape 1 — Découverte des modèles disponibles."""
    print("\n" + "=" * 70)
    print("📋 ÉTAPE 1 — Découverte des modèles Bedrock")
    print("=" * 70)

    resp = bedrock.list_foundation_models()
    models = resp["modelSummaries"]
    print(f"  {len(models)} modèles disponibles :")
    for m in models[:8]:
        stream = "✓" if m.get("responseStreamingSupported") else "✗"
        print(f"    • {m['modelId']:50s} ({m['providerName']}) stream={stream}")

    if len(models) > 8:
        print(f"    ... et {len(models) - 8} autres")

    # Détails d'un modèle spécifique
    detail = bedrock.get_foundation_model(
        modelIdentifier="anthropic.claude-3-sonnet-20240229-v1:0"
    )["modelDetails"]
    print(f"\n  Détail Claude 3 Sonnet :")
    print(f"    Provider  : {detail['providerName']}")
    print(f"    Input     : {detail['inputModalities']}")
    print(f"    Output    : {detail['outputModalities']}")
    print(f"    Streaming : {detail['responseStreamingSupported']}")

    return models


def step_2_create_guardrail(bedrock):
    """Étape 2 — Création d'un guardrail de sécurité."""
    print("\n" + "=" * 70)
    print("🛡️  ÉTAPE 2 — Création du guardrail de sécurité")
    print("=" * 70)

    resp = bedrock.create_guardrail(
        name=f"project-guardrail-{uuid.uuid4().hex[:6]}",
        description="Filtre les données sensibles (PII, credentials) dans les conversations",
        blockedInputMessaging="Désolé, votre message contient des informations sensibles qui ne peuvent pas être traitées.",
        blockedOutputsMessaging="La réponse a été bloquée car elle contenait des informations sensibles.",
        wordPolicyConfig={
            "wordsConfig": [
                {"text": "password"},
                {"text": "secret_key"},
                {"text": "api_key"},
            ],
            "managedWordListsConfig": [],
        },
    )
    guardrail_id = resp["guardrailId"]
    print(f"  Guardrail créé : {guardrail_id}")
    print(f"  Version        : {resp['version']}")

    # Vérifier
    detail = bedrock.get_guardrail(guardrailIdentifier=guardrail_id)
    print(f"  Statut         : {detail['status']}")
    print(f"  Description    : {detail['description']}")

    return guardrail_id


def step_3_upload_documents(s3, bucket):
    """Étape 3 — Upload des documents dans S3."""
    print("\n" + "=" * 70)
    print("📦 ÉTAPE 3 — Upload des documents dans S3")
    print("=" * 70)

    # Créer le bucket
    try:
        s3.create_bucket(Bucket=bucket)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    print(f"  Bucket : s3://{bucket}/")

    # Upload chaque document
    for key, content in KNOWLEDGE_DOCS.items():
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )
        print(f"  ↑ {key} ({len(content)} chars)")

    # Vérifier avec list_objects_v2
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="docs/")
    print(f"\n  {resp['KeyCount']} objets dans s3://{bucket}/docs/")

    # upload_fileobj pour un fichier supplémentaire
    summary = "Résumé du projet : application IA avec RAG, guardrails, et monitoring CloudWatch."
    s3.upload_fileobj(
        io.BytesIO(summary.encode("utf-8")),
        bucket,
        "docs/summary.txt",
    )
    print(f"  ↑ docs/summary.txt (via upload_fileobj)")

    return bucket


def step_4_create_knowledge_base(agent, bucket):
    """Étape 4 — Création de la Knowledge Base et du Data Source."""
    print("\n" + "=" * 70)
    print("🧠 ÉTAPE 4 — Création de la Knowledge Base")
    print("=" * 70)

    # Créer la KB
    resp = agent.create_knowledge_base(
        name="project-knowledge-base",
        description="Base de connaissances du projet IA — architecture, déploiement, sécurité",
        roleArn="arn:aws:iam::000000000000:role/bedrock-kb-role",
        knowledgeBaseConfiguration={
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
            },
        },
    )
    kb_id = resp["knowledgeBase"]["knowledgeBaseId"]
    print(f"  Knowledge Base : {kb_id}")
    print(f"  Statut         : {resp['knowledgeBase']['status']}")

    # Créer le Data Source (lié au bucket S3)
    resp = agent.create_data_source(
        knowledgeBaseId=kb_id,
        name="project-s3-source",
        description=f"Documents depuis s3://{bucket}/docs/",
        dataSourceConfiguration={
            "type": "S3",
            "s3Configuration": {"bucketArn": f"arn:aws:s3:::{bucket}"},
        },
    )
    ds_id = resp["dataSource"]["dataSourceId"]
    print(f"  Data Source    : {ds_id}")
    print(f"  Statut         : {resp['dataSource']['status']}")

    return kb_id, ds_id


def step_5_ingest_documents(agent, kb_id, ds_id, bucket):
    """Étape 5 — Ingestion des documents dans pgvector."""
    print("\n" + "=" * 70)
    print("⚙️  ÉTAPE 5 — Ingestion S3 → pgvector (embeddings)")
    print("=" * 70)

    # Utilise le ds_id retourné par create_data_source (l'ingestion résout le bucket via la config S3)
    resp = agent.start_ingestion_job(
        knowledgeBaseId=kb_id,
        dataSourceId=ds_id,
        description="Ingestion initiale des documents du projet",
    )
    job_id = resp["ingestionJob"]["ingestionJobId"]
    print(f"  Job lancé : {job_id}")
    print(f"  Statut    : {resp['ingestionJob']['status']}")

    # Attendre la fin
    print("  Attente de l'ingestion", end="", flush=True)
    for _ in range(15):
        time.sleep(2)
        print(".", end="", flush=True)
        resp = agent.get_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            ingestionJobId=job_id,
        )
        status = resp["ingestionJob"]["status"]
        if status in ("COMPLETE", "FAILED", "STOPPED"):
            break
    print()

    stats = resp["ingestionJob"].get("statistics", {})
    print(f"  Statut final   : {status}")
    print(f"  Scannés        : {stats.get('numberOfDocumentsScanned', '?')}")
    print(f"  Indexés        : {stats.get('numberOfNewDocumentsIndexed', '?')}")
    print(f"  Échoués        : {stats.get('numberOfDocumentsFailed', '?')}")

    # Lister les jobs
    resp = agent.list_ingestion_jobs(
        knowledgeBaseId=kb_id,
        dataSourceId=ds_id,
    )
    print(f"  Total jobs     : {len(resp.get('ingestionJobSummaries', []))}")

    return job_id


def step_6_semantic_search(agent_runtime, kb_id):
    """Étape 6 — Recherche sémantique dans la Knowledge Base."""
    print("\n" + "=" * 70)
    print("🔍 ÉTAPE 6 — Recherche sémantique (Retrieve)")
    print("=" * 70)

    queries = [
        "Comment est déployée l'application ?",
        "Quels sont les mécanismes de sécurité ?",
        "Quel modèle LLM est utilisé ?",
    ]

    for query in queries:
        print(f"\n  Q: \"{query}\"")
        resp = agent_runtime.retrieve(
            knowledgeBaseId=kb_id,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {"numberOfResults": 2},
            },
        )
        results = resp.get("retrievalResults", [])
        if results:
            for i, r in enumerate(results):
                score = r.get("score", 0)
                text = r["content"]["text"][:100].replace("\n", " ")
                uri = r.get("location", {}).get("s3Location", {}).get("uri", "?")
                print(f"    [{i+1}] score={score:.3f}  {uri}")
                print(f"        {text}...")
        else:
            print("    (aucun résultat — les embeddings n'ont peut-être pas été générés)")


def step_7_rag(agent_runtime, kb_id, model_id):
    """Étape 7 — RAG : Retrieve & Generate."""
    print("\n" + "=" * 70)
    print("🤖 ÉTAPE 7 — RAG (Retrieve & Generate)")
    print("=" * 70)

    question = "Décris l'architecture technique et la stratégie de déploiement du projet."
    print(f"  Q: \"{question}\"")

    resp = agent_runtime.retrieve_and_generate(
        input={"text": question},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": kb_id,
                "modelArn": f"arn:aws:bedrock:us-east-1::foundation-model/{model_id}",
                "retrievalConfiguration": {
                    "vectorSearchConfiguration": {"numberOfResults": 3},
                },
            },
        },
    )

    answer = resp.get("output", {}).get("text", "(pas de réponse)")
    citations = resp.get("citations", [])
    print(f"\n  Réponse ({len(answer)} chars) :")
    for line in answer[:500].split(". "):
        print(f"    {line.strip()}.")
    if len(answer) > 500:
        print(f"    ...")
    print(f"\n  {len(citations)} citation(s)")


def step_8_conversation(bedrock_runtime, model_id):
    """Étape 8 — Conversation directe avec le LLM."""
    print("\n" + "=" * 70)
    print("💬 ÉTAPE 8 — Conversation directe (Converse + InvokeModel)")
    print("=" * 70)

    # --- Converse API (format moderne) ---
    print("\n  [Converse API]")
    resp = bedrock_runtime.converse(
        modelId=model_id,
        messages=[
            {"role": "user", "content": [{"text": "Explique en 2 phrases ce qu'est Amazon Bedrock."}]},
        ],
        system=[{"text": "Tu es un expert AWS. Réponds de manière concise en français."}],
        inferenceConfig={"maxTokens": 200, "temperature": 0.3},
    )
    text = resp["output"]["message"]["content"][0]["text"]
    usage = resp.get("usage", {})
    print(f"  Réponse : {text[:200]}")
    print(f"  Tokens  : in={usage.get('inputTokens', '?')} out={usage.get('outputTokens', '?')}")
    print(f"  Stop    : {resp.get('stopReason', '?')}")

    # --- Multi-turn ---
    print("\n  [Multi-turn]")
    resp = bedrock_runtime.converse(
        modelId=model_id,
        messages=[
            {"role": "user", "content": [{"text": "Mon nom est Alice."}]},
            {"role": "assistant", "content": [{"text": "Bonjour Alice ! Comment puis-je vous aider ?"}]},
            {"role": "user", "content": [{"text": "Quel est mon nom ?"}]},
        ],
        inferenceConfig={"maxTokens": 50, "temperature": 0.0},
    )
    print(f"  Réponse : {resp['output']['message']['content'][0]['text']}")

    # --- InvokeModel (format Anthropic Messages) ---
    print("\n  [InvokeModel — format Anthropic]")
    body = json.dumps({
        "messages": [{"role": "user", "content": "Dis bonjour en une phrase."}],
        "max_tokens": 50,
        "anthropic_version": "bedrock-2023-05-31",
    })
    resp = bedrock_runtime.invoke_model(
        modelId=model_id, body=body, contentType="application/json",
    )
    result = json.loads(resp["body"].read())
    print(f"  Réponse : {result['content'][0]['text']}")
    print(f"  Tokens  : in={result['usage']['input_tokens']} out={result['usage']['output_tokens']}")


def step_9_count_tokens(bedrock_runtime, model_id):
    """Étape 9 — Comptage de tokens."""
    print("\n" + "=" * 70)
    print("🔢 ÉTAPE 9 — Comptage de tokens (CountTokens)")
    print("=" * 70)

    texts = [
        "Bonjour",
        "Amazon Bedrock est un service AWS pour l'inférence LLM.",
        "Lorem ipsum " * 100,
    ]

    for text in texts:
        resp = bedrock_runtime.count_tokens(
            modelId=model_id,
            input={"converse": {
                "messages": [{"role": "user", "content": [{"text": text}]}],
            }},
        )
        preview = text[:60].replace("\n", " ")
        print(f"  \"{preview}{'...' if len(text) > 60 else ''}\"  →  {resp['inputTokens']} tokens")


def step_10_test_guardrail(bedrock_runtime, guardrail_id):
    """Étape 10 — Test du guardrail."""
    print("\n" + "=" * 70)
    print("🛡️  ÉTAPE 10 — Test du guardrail (ApplyGuardrail)")
    print("=" * 70)

    test_cases = [
        ("Quelle est la météo aujourd'hui ?", "NONE"),
        ("Mon password est abc123 et ma secret_key est XYZ", "GUARDRAIL_INTERVENED"),
        ("Voici mon api_key: sk-1234567890", "GUARDRAIL_INTERVENED"),
        ("Explique-moi l'architecture du projet", "NONE"),
    ]

    for text, expected in test_cases:
        resp = bedrock_runtime.apply_guardrail(
            guardrailIdentifier=guardrail_id,
            guardrailVersion="DRAFT",
            source="INPUT",
            content=[{"text": {"text": text}}],
        )
        action = resp["action"]
        icon = "✅" if action == expected else "❌"
        print(f"  {icon} \"{text[:60]}\"")
        print(f"     Action: {action}  (attendu: {expected})")


def step_11_cleanup(s3, agent, bedrock, bucket, kb_id, ds_id, guardrail_id):
    """Étape 11 — Nettoyage."""
    print("\n" + "=" * 70)
    print("🧹 ÉTAPE 11 — Nettoyage")
    print("=" * 70)

    # Supprimer les objets S3
    try:
        resp = s3.list_objects_v2(Bucket=bucket)
        objs = resp.get("Contents", [])
        if objs:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in objs], "Quiet": True},
            )
        s3.delete_bucket(Bucket=bucket)
        print(f"  ✓ Bucket s3://{bucket}/ supprimé ({len(objs)} objets)")
    except Exception as e:
        print(f"  ⚠ Bucket: {e}")

    # Supprimer data source + KB
    try:
        agent.delete_data_source(knowledgeBaseId=kb_id, dataSourceId=ds_id)
        print(f"  ✓ Data Source {ds_id} supprimé")
    except Exception:
        pass
    try:
        agent.delete_knowledge_base(knowledgeBaseId=kb_id)
        print(f"  ✓ Knowledge Base {kb_id} supprimée")
    except Exception:
        pass

    # Supprimer le guardrail
    try:
        bedrock.delete_guardrail(guardrailIdentifier=guardrail_id)
        print(f"  ✓ Guardrail {guardrail_id} supprimé")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Projet IA/ML avec AWS Bedrock — demo MiniStack",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--aws", action="store_true", help="Cibler AWS réel au lieu de MiniStack")
    parser.add_argument("--endpoint", default=None, help=f"Endpoint custom (défaut: {MINISTACK_ENDPOINT})")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Model ID (défaut: {DEFAULT_MODEL})")
    parser.add_argument("--no-cleanup", action="store_true", help="Ne pas nettoyer à la fin")
    args = parser.parse_args()

    endpoint = None if args.aws else (args.endpoint or MINISTACK_ENDPOINT)
    target = "AWS" if args.aws else f"MiniStack ({endpoint})"

    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║        Projet IA/ML avec AWS Bedrock — Pipeline RAG complet        ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print(f"║  Cible   : {target:57s} ║")
    print(f"║  Région  : {args.region:57s} ║")
    print(f"║  Modèle  : {args.model:57s} ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")

    # Créer les clients
    bedrock = make_client("bedrock", endpoint, args.region)
    bedrock_runtime = make_client("bedrock-runtime", endpoint, args.region)
    bedrock_agent = make_client("bedrock-agent", endpoint, args.region)
    bedrock_agent_runtime = make_client("bedrock-agent-runtime", endpoint, args.region)
    s3 = make_client("s3", endpoint, args.region)

    bucket = f"ai-project-{uuid.uuid4().hex[:8]}"
    kb_id = ds_id = guardrail_id = None

    try:
        # Pipeline complet
        step_1_discover_models(bedrock)
        guardrail_id = step_2_create_guardrail(bedrock)
        step_3_upload_documents(s3, bucket)
        kb_id, ds_id = step_4_create_knowledge_base(bedrock_agent, bucket)
        step_5_ingest_documents(bedrock_agent, kb_id, ds_id, bucket)
        step_6_semantic_search(bedrock_agent_runtime, kb_id)
        step_7_rag(bedrock_agent_runtime, kb_id, args.model)
        step_8_conversation(bedrock_runtime, args.model)
        step_9_count_tokens(bedrock_runtime, args.model)
        step_10_test_guardrail(bedrock_runtime, guardrail_id)

        print("\n" + "=" * 70)
        print("✅ PIPELINE COMPLET — Toutes les étapes ont réussi !")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()

    finally:
        if not args.no_cleanup:
            step_11_cleanup(s3, bedrock_agent, bedrock, bucket, kb_id, ds_id, guardrail_id)


if __name__ == "__main__":
    main()
