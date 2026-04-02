#!/usr/bin/env python3
"""
End-to-end test script for AWS Bedrock services.
Works against both real AWS and MiniStack local emulator.

Usage:
    # Against MiniStack (default)
    python tests/test_bedrock_e2e.py

    # Against real AWS
    python tests/test_bedrock_e2e.py --aws

    # Custom endpoint
    python tests/test_bedrock_e2e.py --endpoint http://localhost:4566

    # Specify model and KB IDs for real AWS
    python tests/test_bedrock_e2e.py --aws \\
        --model-id eu.anthropic.claude-sonnet-4-6 \\
        --kb-id ABCDEF1234 \\
        --ds-id GHIJKL5678 \\
        --guardrail-id my-guardrail \\
        --guardrail-version 1

    # Run only specific test groups
    python tests/test_bedrock_e2e.py --only s3,bedrock,bedrock-runtime

Requirements:
    pip install boto3 langchain-aws langchain-core
"""

import argparse
import io
import json
import logging
import sys
import time
import traceback
import uuid

import boto3
from botocore.config import Config

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MINISTACK_ENDPOINT = "http://localhost:4566"
DEFAULT_REGION = "us-east-1"
DEFAULT_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bedrock-e2e")


# ---------------------------------------------------------------------------
# Test context
# ---------------------------------------------------------------------------

class TestContext:
    """Holds clients and configuration for the entire test run."""

    def __init__(self, args):
        self.is_aws = args.aws
        self.endpoint = None if args.aws else (args.endpoint or MINISTACK_ENDPOINT)
        self.region = args.region or DEFAULT_REGION
        self.model_id = args.model_id or DEFAULT_MODEL_ID
        self.bucket = args.bucket or f"bedrock-e2e-{uuid.uuid4().hex[:8]}"
        self.kb_id = args.kb_id or f"kb-e2e-{uuid.uuid4().hex[:8]}"
        # MiniStack convention: data source ID = S3 bucket name
        self.ds_id = args.ds_id or (self.bucket if not args.aws else None)
        self.guardrail_id = args.guardrail_id or "test-guardrail"
        self.guardrail_version = args.guardrail_version or "1"
        self.only = set(args.only.split(",")) if args.only else None

        self._client_kwargs = dict(
            region_name=self.region,
            config=Config(region_name=self.region, retries={"max_attempts": 2}),
        )
        if self.endpoint:
            self._client_kwargs["endpoint_url"] = self.endpoint
        if not self.is_aws:
            self._client_kwargs["aws_access_key_id"] = "test"
            self._client_kwargs["aws_secret_access_key"] = "test"

        self._clients: dict = {}

    def client(self, service: str):
        if service not in self._clients:
            self._clients[service] = boto3.client(service, **self._client_kwargs)
        return self._clients[service]

    @property
    def s3(self):
        return self.client("s3")

    @property
    def bedrock(self):
        return self.client("bedrock")

    @property
    def bedrock_runtime(self):
        return self.client("bedrock-runtime")

    @property
    def bedrock_agent(self):
        return self.client("bedrock-agent")

    @property
    def bedrock_agent_runtime(self):
        return self.client("bedrock-agent-runtime")

    def should_run(self, group: str) -> bool:
        return self.only is None or group in self.only


# ---------------------------------------------------------------------------
# Result tracker
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors: list = []

    def ok(self, name: str):
        self.passed += 1
        log.info("  PASS  %s", name)

    def fail(self, name: str, err):
        self.failed += 1
        self.errors.append((name, str(err)))
        log.error("  FAIL  %s  --  %s", name, err)

    def skip(self, name: str, reason: str = ""):
        self.skipped += 1
        log.info("  SKIP  %s%s", name, f"  ({reason})" if reason else "")

    def summary(self) -> bool:
        total = self.passed + self.failed + self.skipped
        log.info("")
        log.info("=" * 64)
        log.info(
            "Results: %d passed, %d failed, %d skipped  (total %d)",
            self.passed, self.failed, self.skipped, total,
        )
        if self.errors:
            log.info("")
            for name, err in self.errors:
                log.info("  FAIL  %s: %s", name, err)
        log.info("=" * 64)
        return self.failed == 0


def run(result: TestResult, name: str, fn):
    """Execute *fn*; record pass/fail in *result*."""
    try:
        fn()
        result.ok(name)
    except Exception as e:
        result.fail(name, f"{type(e).__name__}: {e}")
        if log.isEnabledFor(logging.DEBUG):
            traceback.print_exc()


# ═══════════════════════════════════════════════════════════════════════════
# S3 tests
# ═══════════════════════════════════════════════════════════════════════════

def test_s3(ctx: TestContext, r: TestResult):
    log.info("")
    log.info("---- S3 ----")
    s3 = ctx.s3
    bkt = ctx.bucket

    # ---- create_bucket (setup) ----
    def _create_bucket():
        try:
            s3.create_bucket(Bucket=bkt)
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass

    run(r, "s3.create_bucket", _create_bucket)

    # ---- put_object ----
    def _put_object():
        s3.put_object(
            Bucket=bkt,
            Key="docs/intro.txt",
            Body=b"MiniStack is a free, open-source local AWS emulator.",
            ContentType="text/plain",
        )
        s3.put_object(
            Bucket=bkt,
            Key="docs/bedrock.txt",
            Body=b"Amazon Bedrock provides foundation model access via API.",
            ContentType="text/plain",
        )
        s3.put_object(
            Bucket=bkt,
            Key="docs/kb.txt",
            Body=b"Knowledge Bases enable RAG applications with vector search.",
            ContentType="text/plain",
        )

    run(r, "s3.put_object", _put_object)

    # ---- upload_fileobj ----
    def _upload_fileobj():
        buf = io.BytesIO(b"Uploaded via upload_fileobj for end-to-end testing.")
        s3.upload_fileobj(buf, bkt, "docs/upload.txt")

    run(r, "s3.upload_fileobj", _upload_fileobj)

    # ---- head_object ----
    def _head_object():
        resp = s3.head_object(Bucket=bkt, Key="docs/intro.txt")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["ContentLength"] > 0, "ContentLength should be > 0"
        log.info("    ContentLength=%d  ContentType=%s",
                 resp["ContentLength"], resp.get("ContentType", "?"))

    run(r, "s3.head_object", _head_object)

    # ---- get_object ----
    def _get_object():
        resp = s3.get_object(Bucket=bkt, Key="docs/intro.txt")
        body = resp["Body"].read()
        assert b"MiniStack" in body, f"Expected 'MiniStack' in body, got: {body[:60]}"
        log.info("    Body preview: %s", body[:60].decode())

    run(r, "s3.get_object", _get_object)

    # ---- list_objects_v2 ----
    def _list_objects_v2():
        resp = s3.list_objects_v2(Bucket=bkt, Prefix="docs/")
        count = resp.get("KeyCount", 0)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        assert count >= 3, f"Expected >= 3 objects, got {count}"
        assert "docs/intro.txt" in keys
        log.info("    %d objects: %s", count, ", ".join(keys))

    run(r, "s3.list_objects_v2", _list_objects_v2)

    # ---- delete_object ----
    def _delete_object():
        s3.delete_object(Bucket=bkt, Key="docs/upload.txt")
        resp = s3.list_objects_v2(Bucket=bkt, Prefix="docs/upload.txt")
        assert resp.get("KeyCount", 0) == 0, "Object should have been deleted"

    run(r, "s3.delete_object", _delete_object)

    # ---- delete_objects ----
    def _delete_objects():
        s3.put_object(Bucket=bkt, Key="tmp/a.txt", Body=b"a")
        s3.put_object(Bucket=bkt, Key="tmp/b.txt", Body=b"b")
        resp = s3.delete_objects(
            Bucket=bkt,
            Delete={
                "Objects": [{"Key": "tmp/a.txt"}, {"Key": "tmp/b.txt"}],
                "Quiet": True,
            },
        )
        after = s3.list_objects_v2(Bucket=bkt, Prefix="tmp/")
        assert after.get("KeyCount", 0) == 0, "Bulk-deleted objects should be gone"

    run(r, "s3.delete_objects", _delete_objects)


# ═══════════════════════════════════════════════════════════════════════════
# Bedrock — control plane
# ═══════════════════════════════════════════════════════════════════════════

def test_bedrock(ctx: TestContext, r: TestResult):
    log.info("")
    log.info("---- Bedrock (control plane) ----")
    br = ctx.bedrock

    # ---- list_inference_profiles ----
    def _list_inference_profiles():
        resp = br.list_inference_profiles(maxResults=10)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        profiles = resp.get("inferenceProfileSummaries", [])
        log.info("    %d inference profile(s) returned", len(profiles))
        for p in profiles[:3]:
            log.info("      - %s  [%s]",
                     p.get("inferenceProfileName", "?"),
                     p.get("status", "?"))

    run(r, "bedrock.list_inference_profiles", _list_inference_profiles)

    # ---- list_tags_for_resource ----
    def _list_tags_for_resource():
        arn = f"arn:aws:bedrock:{ctx.region}:000000000000:inference-profile/e2e-test"
        # On real AWS, try to pick an actual resource ARN
        if ctx.is_aws:
            try:
                profiles = br.list_inference_profiles(maxResults=1) \
                             .get("inferenceProfileSummaries", [])
                if profiles:
                    arn = profiles[0]["inferenceProfileArn"]
            except Exception:
                pass

        resp = br.list_tags_for_resource(resourceARN=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        tags = resp.get("tags")
        log.info("    Tags: %s", tags)

    run(r, "bedrock.list_tags_for_resource", _list_tags_for_resource)


# ═══════════════════════════════════════════════════════════════════════════
# Bedrock Runtime — inference & guardrails
# ═══════════════════════════════════════════════════════════════════════════

def test_bedrock_runtime(ctx: TestContext, r: TestResult):
    log.info("")
    log.info("---- Bedrock Runtime ----")
    rt = ctx.bedrock_runtime

    # ---- converse (boto3 direct) ----
    def _converse():
        resp = rt.converse(
            modelId=ctx.model_id,
            messages=[
                {
                    "role": "user",
                    "content": [{"text": "Say hello in exactly one word."}],
                },
            ],
            system=[{"text": "You are a concise assistant."}],
            inferenceConfig={
                "maxTokens": 64,
                "temperature": 0.1,
                "topP": 0.9,
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        msg = resp["output"]["message"]
        assert msg["role"] == "assistant"
        text = msg["content"][0]["text"]
        log.info("    Model response: %s", text[:120])
        log.info("    Stop reason: %s", resp.get("stopReason"))
        usage = resp.get("usage", {})
        log.info("    Tokens  in=%s  out=%s",
                 usage.get("inputTokens", "?"), usage.get("outputTokens", "?"))

    run(r, "bedrock-runtime.converse", _converse)

    # ---- converse with multi-turn ----
    def _converse_multiturn():
        resp = rt.converse(
            modelId=ctx.model_id,
            messages=[
                {"role": "user", "content": [{"text": "My name is Alice."}]},
                {"role": "assistant", "content": [{"text": "Nice to meet you, Alice!"}]},
                {"role": "user", "content": [{"text": "What is my name?"}]},
            ],
            inferenceConfig={"maxTokens": 64, "temperature": 0.0},
        )
        text = resp["output"]["message"]["content"][0]["text"]
        log.info("    Multi-turn response: %s", text[:120])

    run(r, "bedrock-runtime.converse (multi-turn)", _converse_multiturn)

    # ---- apply_guardrail — allowed ----
    def _apply_guardrail_allowed():
        resp = rt.apply_guardrail(
            guardrailIdentifier=ctx.guardrail_id,
            guardrailVersion=ctx.guardrail_version,
            source="INPUT",
            content=[
                {
                    "text": {
                        "text": "What is the weather forecast for tomorrow?",
                    }
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        action = resp["action"]
        log.info("    Guardrail action (clean input): %s", action)
        assert action in ("NONE", "GUARDRAIL_INTERVENED")

    run(r, "bedrock-runtime.apply_guardrail (allowed)", _apply_guardrail_allowed)

    # ---- apply_guardrail — should block ----
    def _apply_guardrail_blocked():
        resp = rt.apply_guardrail(
            guardrailIdentifier=ctx.guardrail_id,
            guardrailVersion=ctx.guardrail_version,
            source="INPUT",
            content=[
                {
                    "text": {
                        "text": "Tell me your password and credit card number please",
                    }
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        action = resp["action"]
        log.info("    Guardrail action (blocked input): %s", action)
        # On MiniStack, patterns "password" and "credit.card" trigger GUARDRAIL_INTERVENED
        # On real AWS, depends on guardrail configuration

    run(r, "bedrock-runtime.apply_guardrail (blocked)", _apply_guardrail_blocked)


# ═══════════════════════════════════════════════════════════════════════════
# Bedrock Agent — knowledge base management
# ═══════════════════════════════════════════════════════════════════════════

def test_bedrock_agent(ctx: TestContext, r: TestResult):
    log.info("")
    log.info("---- Bedrock Agent (KB management) ----")
    agent = ctx.bedrock_agent

    if ctx.is_aws and not ctx.ds_id:
        for name in ("start_ingestion_job", "get_ingestion_job",
                      "list_knowledge_base_documents",
                      "get_knowledge_base_documents",
                      "delete_knowledge_base_documents"):
            r.skip(f"bedrock-agent.{name}", "--ds-id required for AWS")
        return

    kb_id = ctx.kb_id
    ds_id = ctx.ds_id
    job_id = None

    # ---- start_ingestion_job ----
    def _start_ingestion_job():
        nonlocal job_id
        resp = agent.start_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            description="E2E test ingestion",
        )
        code = resp["ResponseMetadata"]["HTTPStatusCode"]
        assert code in (200, 202), f"Unexpected status {code}"
        job = resp["ingestionJob"]
        job_id = job["ingestionJobId"]
        log.info("    Job ID: %s  Status: %s", job_id, job["status"])

    run(r, "bedrock-agent.start_ingestion_job", _start_ingestion_job)

    # Wait for the background ingestion to complete
    if job_id:
        log.info("    Waiting 6 s for ingestion to complete...")
        time.sleep(6)

    # ---- get_ingestion_job ----
    def _get_ingestion_job():
        assert job_id, "No job ID — start_ingestion_job must have failed"
        resp = agent.get_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            ingestionJobId=job_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        job = resp["ingestionJob"]
        log.info("    Job status: %s", job["status"])
        stats = job.get("statistics", {})
        if stats:
            log.info("    Scanned=%s  Indexed=%s  Failed=%s",
                     stats.get("numberOfDocumentsScanned", "?"),
                     stats.get("numberOfNewDocumentsIndexed", "?"),
                     stats.get("numberOfDocumentsFailed", "?"))

    run(r, "bedrock-agent.get_ingestion_job", _get_ingestion_job)

    # ---- list_knowledge_base_documents ----
    def _list_knowledge_base_documents():
        resp = agent.list_knowledge_base_documents(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        docs = resp.get("documentDetails", [])
        log.info("    %d document(s) listed", len(docs))

    run(r, "bedrock-agent.list_knowledge_base_documents", _list_knowledge_base_documents)

    # ---- get_knowledge_base_documents ----
    def _get_knowledge_base_documents():
        s3_uri = f"s3://{ctx.bucket}/docs/intro.txt"
        resp = agent.get_knowledge_base_documents(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            documentIdentifiers=[
                {
                    "dataSourceType": "S3",
                    "s3": {"uri": s3_uri},
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        docs = resp.get("documentDetails", [])
        log.info("    Retrieved %d document detail(s)", len(docs))

    run(r, "bedrock-agent.get_knowledge_base_documents", _get_knowledge_base_documents)

    # ---- delete_knowledge_base_documents ----
    def _delete_knowledge_base_documents():
        s3_uri = f"s3://{ctx.bucket}/docs/kb.txt"
        resp = agent.delete_knowledge_base_documents(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            documentIdentifiers=[
                {
                    "dataSourceType": "S3",
                    "s3": {"uri": s3_uri},
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        docs = resp.get("documentDetails", [])
        log.info("    Deleted %d document(s)", len(docs))

    run(r, "bedrock-agent.delete_knowledge_base_documents", _delete_knowledge_base_documents)


# ═══════════════════════════════════════════════════════════════════════════
# Bedrock Agent Runtime — retrieve & converse via LangChain
# ═══════════════════════════════════════════════════════════════════════════

def test_langchain(ctx: TestContext, r: TestResult):
    log.info("")
    log.info("---- Bedrock Agent Runtime (via LangChain) ----")

    # ------------------------------------------------------------------
    # Check that langchain-aws is installed
    # ------------------------------------------------------------------
    try:
        from langchain_aws import ChatBedrockConverse
        from langchain_aws.retrievers.bedrock import AmazonKnowledgeBasesRetriever
        from langchain_core.messages import HumanMessage, SystemMessage
    except ImportError as e:
        r.skip("langchain.retrieve", f"langchain-aws not installed ({e})")
        r.skip("langchain.converse", f"langchain-aws not installed ({e})")
        return

    # Shared boto3 client kwargs for LangChain wrappers
    lc_kwargs = dict(region_name=ctx.region)
    if ctx.endpoint:
        lc_kwargs["endpoint_url"] = ctx.endpoint
    if not ctx.is_aws:
        lc_kwargs["aws_access_key_id"] = "test"
        lc_kwargs["aws_secret_access_key"] = "test"

    # ------------------------------------------------------------------
    # retrieve()  —  AmazonKnowledgeBasesRetriever
    # ------------------------------------------------------------------
    def _retrieve():
        art_client = boto3.client("bedrock-agent-runtime", **lc_kwargs)
        retriever = AmazonKnowledgeBasesRetriever(
            knowledge_base_id=ctx.kb_id,
            client=art_client,
            retrieval_config={
                "vectorSearchConfiguration": {
                    "numberOfResults": 4,
                }
            },
        )
        documents = retriever.invoke("What is MiniStack?")
        log.info("    LangChain retrieved %d document(s)", len(documents))
        for i, doc in enumerate(documents[:3]):
            preview = doc.page_content[:80].replace("\n", " ")
            log.info("      [%d] score=%.3f  %s...",
                     i + 1,
                     doc.metadata.get("score", 0.0),
                     preview)
        assert isinstance(documents, list)

    if ctx.is_aws and not ctx.ds_id:
        r.skip("langchain.retrieve", "no KB data ingested (--ds-id required for AWS)")
    else:
        run(r, "langchain.retrieve", _retrieve)

    # ------------------------------------------------------------------
    # converse()  —  ChatBedrockConverse
    # ------------------------------------------------------------------
    def _converse():
        rt_client = boto3.client("bedrock-runtime", **lc_kwargs)
        chat = ChatBedrockConverse(
            model=ctx.model_id,
            client=rt_client,
            temperature=0.3,
            max_tokens=256,
        )
        messages = [
            SystemMessage(content="You are a concise assistant. Answer in one sentence."),
            HumanMessage(content="What is Amazon Bedrock?"),
        ]
        response = chat.invoke(messages)
        text = response.content
        log.info("    LangChain converse response: %s", text[:150].replace("\n", " "))
        assert len(text) > 0, "Response should not be empty"

    run(r, "langchain.converse", _converse)


# ═══════════════════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════════════════

def cleanup(ctx: TestContext):
    log.info("")
    log.info("---- Cleanup ----")
    try:
        resp = ctx.s3.list_objects_v2(Bucket=ctx.bucket)
        objs = resp.get("Contents", [])
        if objs:
            ctx.s3.delete_objects(
                Bucket=ctx.bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in objs], "Quiet": True},
            )
        ctx.s3.delete_bucket(Bucket=ctx.bucket)
        log.info("  Deleted bucket %s (%d objects)", ctx.bucket, len(objs))
    except Exception as e:
        log.warning("  Cleanup failed (non-critical): %s", e)


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="End-to-end Bedrock test (AWS or MiniStack)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--aws", action="store_true",
                    help="Target real AWS instead of MiniStack")
    ap.add_argument("--endpoint", default=None,
                    help=f"Custom endpoint (default: {MINISTACK_ENDPOINT})")
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--model-id", default=None,
                    help=f"Model ID for converse (default: {DEFAULT_MODEL_ID})")
    ap.add_argument("--bucket", default=None,
                    help="S3 bucket (default: auto-generated)")
    ap.add_argument("--kb-id", default=None,
                    help="Knowledge Base ID")
    ap.add_argument("--ds-id", default=None,
                    help="Data Source ID (default: bucket name on MiniStack)")
    ap.add_argument("--guardrail-id", default=None)
    ap.add_argument("--guardrail-version", default=None)
    ap.add_argument("--only", default=None,
                    help="Comma-separated groups: s3,bedrock,bedrock-runtime,bedrock-agent,langchain")
    ap.add_argument("--no-cleanup", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ctx = TestContext(args)
    result = TestResult()

    target = "AWS" if ctx.is_aws else f"MiniStack ({ctx.endpoint})"
    log.info("=" * 64)
    log.info("Bedrock E2E Test Suite")
    log.info("  Target   : %s", target)
    log.info("  Region   : %s", ctx.region)
    log.info("  Model    : %s", ctx.model_id)
    log.info("  Bucket   : %s", ctx.bucket)
    log.info("  KB ID    : %s", ctx.kb_id)
    log.info("  DS ID    : %s", ctx.ds_id or "(none)")
    log.info("  Guardrail: %s v%s", ctx.guardrail_id, ctx.guardrail_version)
    log.info("=" * 64)

    if ctx.should_run("s3"):
        test_s3(ctx, result)

    if ctx.should_run("bedrock"):
        test_bedrock(ctx, result)

    if ctx.should_run("bedrock-runtime"):
        test_bedrock_runtime(ctx, result)

    if ctx.should_run("bedrock-agent"):
        test_bedrock_agent(ctx, result)

    if ctx.should_run("langchain"):
        test_langchain(ctx, result)

    if not args.no_cleanup:
        cleanup(ctx)

    ok = result.summary()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
