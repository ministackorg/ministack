"""
AWS Bedrock Agent Runtime Service Emulator.
REST-based API for Knowledge Base retrieval and RAG (Retrieve and Generate).
Supports: Retrieve, RetrieveAndGenerate.

Uses pgvector for semantic search and LiteLLM for embeddings + inference.
Requires pgvector and LiteLLM to be running — returns ServiceUnavailableException if unavailable.
"""

import json
import logging
import os
import re

from ministack.core.responses import error_response_json, json_response, new_uuid, now_iso

logger = logging.getLogger("bedrock-agent-runtime")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
LITELLM_BASE_URL = os.environ.get("LITELLM_BASE_URL", "http://litellm:4000")
PGVECTOR_HOST = os.environ.get("PGVECTOR_HOST", "pgvector")
PGVECTOR_PORT = os.environ.get("PGVECTOR_PORT", "5432")
PGVECTOR_DB = os.environ.get("PGVECTOR_DB", "bedrock_kb")
PGVECTOR_USER = os.environ.get("PGVECTOR_USER", "bedrock")
PGVECTOR_PASSWORD = os.environ.get("PGVECTOR_PASSWORD", "bedrock")

# ---------------------------------------------------------------------------
# Path routing patterns
# ---------------------------------------------------------------------------

# POST /knowledgebases/{kbId}/retrieve
_RE_RETRIEVE = re.compile(r"^/knowledgebases/([^/]+)/retrieve$")
_RE_RETRIEVE_AND_GENERATE = re.compile(r"^/knowledgebases/([^/]+)/retrieve-and-generate$")
_RE_RETRIEVE_AND_GENERATE_TOP = re.compile(r"^/retrieveAndGenerate/?$")
_RE_RERANK = re.compile(r"^/rerank/?$")


async def handle_request(method, path, headers, body, query_params):
    """Main entry point for Bedrock Agent Runtime requests."""
    # Retrieve
    m = _RE_RETRIEVE.match(path)
    if m and method == "POST":
        return await _retrieve(m.group(1), body)

    # RetrieveAndGenerate — path-prefixed form
    m = _RE_RETRIEVE_AND_GENERATE.match(path)
    if m and method == "POST":
        return await _retrieve_and_generate(m.group(1), body)

    # RetrieveAndGenerate — top-level form (boto3 sends POST /retrieveAndGenerate)
    if _RE_RETRIEVE_AND_GENERATE_TOP.match(path) and method == "POST":
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return error_response_json("ValidationException", "Invalid JSON body", 400)
        # Extract KB ID from request body
        kb_id = (data.get("retrieveAndGenerateConfiguration", {})
                     .get("knowledgeBaseConfiguration", {})
                     .get("knowledgeBaseId", ""))
        return await _retrieve_and_generate(kb_id, body)

    # Rerank
    if _RE_RERANK.match(path) and method == "POST":
        return await _rerank(body)

    return error_response_json("UnrecognizedClientException",
                               f"Unrecognized operation: {method} {path}", 400)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def _get_pg_connection():
    """Get an asyncpg connection to pgvector."""
    import asyncpg
    try:
        conn = await asyncpg.connect(
            host=PGVECTOR_HOST,
            port=int(PGVECTOR_PORT),
            database=PGVECTOR_DB,
            user=PGVECTOR_USER,
            password=PGVECTOR_PASSWORD,
        )
        return conn
    except (OSError, asyncpg.PostgresError) as e:
        logger.error("Failed to connect to pgvector at %s:%s: %s", PGVECTOR_HOST, PGVECTOR_PORT, e)
        raise


async def _generate_embedding(text: str) -> list:
    """Generate embedding vector via LiteLLM embeddings API."""
    import aiohttp
    from ministack.services.bedrock import get_models_config

    config = get_models_config()
    embedding_model = config.get("embedding_model", "nomic-embed-text")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{LITELLM_BASE_URL}/v1/embeddings",
                json={"model": embedding_model, "input": text},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status != 200:
                    error_body = await resp.text()
                    raise RuntimeError(f"LiteLLM embeddings returned {resp.status}: {error_body}")
                result = await resp.json()
                return result["data"][0]["embedding"]
    except (aiohttp.ClientError, OSError) as e:
        raise RuntimeError(f"Failed to connect to LiteLLM for embeddings: {e}") from e


# ---------------------------------------------------------------------------
# API handlers
# ---------------------------------------------------------------------------

async def _retrieve(kb_id: str, body: bytes):
    """
    Retrieve — semantic search over Knowledge Base documents.
    Generates embedding for the query, then finds nearest neighbors in pgvector.
    """
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    retrieval_query = data.get("retrievalQuery", {})
    query_text = retrieval_query.get("text", "")
    if not query_text:
        return error_response_json("ValidationException", "retrievalQuery.text is required", 400)

    # Retrieval configuration
    retrieval_config = data.get("retrievalConfiguration", {})
    vector_config = retrieval_config.get("vectorSearchConfiguration", {})
    number_of_results = vector_config.get("numberOfResults", 5)

    # Generate query embedding
    try:
        query_embedding = await _generate_embedding(query_text)
    except RuntimeError as e:
        return error_response_json("ServiceUnavailableException",
                                   f"Embedding service unavailable: {e}", 503)

    # Search pgvector
    try:
        conn = await _get_pg_connection()
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"pgvector is unavailable: {e}", 503)

    try:
        # Use cosine distance for similarity search
        embedding_str = str(query_embedding)
        rows = await conn.fetch(
            """
            SELECT id, s3_uri, content, metadata,
                   1 - (embedding <=> $1::vector) AS score
            FROM kb_documents
            WHERE knowledge_base_id = $2
            ORDER BY embedding <=> $1::vector
            LIMIT $3
            """,
            embedding_str, kb_id, number_of_results,
        )
    finally:
        await conn.close()

    # Build Bedrock RetrieveResponse
    retrieval_results = []
    for row in rows:
        metadata = {}
        try:
            metadata = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"]
        except (json.JSONDecodeError, TypeError):
            pass

        retrieval_results.append({
            "content": {
                "text": row["content"][:10000] if row["content"] else "",
            },
            "location": {
                "type": "S3",
                "s3Location": {
                    "uri": row["s3_uri"] or "",
                },
            },
            "metadata": metadata,
            "score": float(row["score"]) if row["score"] is not None else 0.0,
        })

    return json_response({
        "retrievalResults": retrieval_results,
        "ResponseMetadata": {
            "RequestId": new_uuid(),
            "HTTPStatusCode": 200,
        },
    })


async def _retrieve_and_generate(kb_id: str, body: bytes):
    """
    RetrieveAndGenerate — RAG: retrieve relevant documents then generate a response.
    Combines vector search with LLM inference.
    """
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    # Extract input
    input_data = data.get("input", {})
    query_text = input_data.get("text", "")
    if not query_text:
        return error_response_json("ValidationException", "input.text is required", 400)

    # Retrieve configuration
    rag_config = data.get("retrieveAndGenerateConfiguration", {})
    kb_config = rag_config.get("knowledgeBaseConfiguration", {})
    model_arn = kb_config.get("modelArn", "")
    number_of_results = kb_config.get("retrievalConfiguration", {}).get(
        "vectorSearchConfiguration", {}).get("numberOfResults", 5)

    # Extract model ID from ARN or use directly
    model_id = model_arn.split("/")[-1] if "/" in model_arn else model_arn

    # Step 1: Retrieve relevant documents
    try:
        query_embedding = await _generate_embedding(query_text)
    except RuntimeError as e:
        return error_response_json("ServiceUnavailableException",
                                   f"Embedding service unavailable: {e}", 503)

    try:
        conn = await _get_pg_connection()
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"pgvector is unavailable: {e}", 503)

    try:
        embedding_str = str(query_embedding)
        rows = await conn.fetch(
            """
            SELECT content, s3_uri,
                   1 - (embedding <=> $1::vector) AS score
            FROM kb_documents
            WHERE knowledge_base_id = $2
            ORDER BY embedding <=> $1::vector
            LIMIT $3
            """,
            embedding_str, kb_id, number_of_results,
        )
    finally:
        await conn.close()

    # Step 2: Build context from retrieved documents
    context_parts = []
    citations = []
    for i, row in enumerate(rows):
        content = row["content"][:5000] if row["content"] else ""
        context_parts.append(f"[Document {i + 1}]: {content}")
        citations.append({
            "retrievedReferences": [{
                "content": {"text": content},
                "location": {
                    "type": "S3",
                    "s3Location": {"uri": row["s3_uri"] or ""},
                },
            }],
            "generatedResponsePart": {
                "textResponsePart": {"span": {"start": 0, "end": 0}, "text": ""},
            },
        })

    context = "\n\n".join(context_parts)

    # Step 3: Generate response via LiteLLM
    from ministack.services.bedrock import resolve_model
    from ministack.services.bedrock_runtime import _call_llm

    local_model = resolve_model(model_id) if model_id else "qwen3.5:2b"
    system_prompt = (
        "You are a helpful assistant. Answer the user's question based on the following context. "
        "If the context doesn't contain relevant information, say so.\n\n"
        f"Context:\n{context}"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": query_text},
    ]

    try:
        result = await _call_llm(local_model, messages, max_tokens=1024, temperature=0.3)
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"Inference backend is unavailable: {e}", 503)

    response_text = result.get("choices", [{}])[0].get("message", {}).get("content", "")

    return json_response({
        "output": {
            "text": response_text,
        },
        "citations": citations,
        "sessionId": new_uuid(),
        "ResponseMetadata": {
            "RequestId": new_uuid(),
            "HTTPStatusCode": 200,
        },
    })


async def _rerank(body: bytes):
    """Rerank — reorder documents by relevance to a query using embeddings."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    queries = data.get("queries", [])
    sources = data.get("sources", [])
    reranking_config = data.get("rerankingConfiguration", {})
    bedrock_config = reranking_config.get("bedrockRerankingConfiguration", {})
    num_results = bedrock_config.get("numberOfResults", len(sources))

    if not queries:
        return error_response_json("ValidationException", "queries is required", 400)
    if not sources:
        return error_response_json("ValidationException", "sources is required", 400)

    # Extract query text
    query_text = ""
    for q in queries:
        if q.get("type") == "TEXT":
            query_text = q.get("textQuery", {}).get("text", "")
            break
        elif isinstance(q, str):
            query_text = q
            break

    # Extract source texts
    source_texts = []
    for s in sources:
        text = ""
        if s.get("type") == "INLINE":
            inline = s.get("inlineDocumentSource", {})
            text = inline.get("textDocument", {}).get("text", "")
            if not text:
                text = json.dumps(inline.get("jsonDocument", {}))
        source_texts.append(text)

    # Generate embeddings for query and all sources, compute cosine similarity
    try:
        query_emb = await _generate_embedding(query_text)
        scored = []
        for i, src_text in enumerate(source_texts):
            src_emb = await _generate_embedding(src_text[:8000])
            # Cosine similarity
            dot = sum(a * b for a, b in zip(query_emb, src_emb))
            mag_q = sum(a * a for a in query_emb) ** 0.5
            mag_s = sum(a * a for a in src_emb) ** 0.5
            score = dot / (mag_q * mag_s) if mag_q > 0 and mag_s > 0 else 0.0
            scored.append({"index": i, "relevanceScore": round(score, 6)})

        scored.sort(key=lambda x: x["relevanceScore"], reverse=True)
        scored = scored[:num_results]
    except RuntimeError as e:
        # Fallback: return sources in order with decreasing scores
        scored = [{"index": i, "relevanceScore": round(1.0 - i * 0.1, 2)}
                  for i in range(min(num_results, len(sources)))]

    return json_response({"results": scored})


def reset():
    """Clear all in-memory state (pgvector data persists separately)."""
    pass


def get_state():
    """Return serializable state for persistence."""
    return {}
