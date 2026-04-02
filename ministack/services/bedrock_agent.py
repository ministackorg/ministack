"""
AWS Bedrock Agent Service Emulator.
REST-based API for Knowledge Base management, ingestion jobs, and document CRUD.
Supports: StartIngestionJob, GetIngestionJob, GetKnowledgeBaseDocuments,
          ListKnowledgeBaseDocuments, DeleteKnowledgeBaseDocuments.

Uses PostgreSQL+pgvector for vector storage and LiteLLM for embeddings.
Requires pgvector and LiteLLM to be running — returns ServiceUnavailableException if unavailable.
"""

import json
import logging
import os
import re
import threading
import time

from ministack.core.responses import error_response_json, json_response, new_uuid, now_iso

logger = logging.getLogger("bedrock-agent")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
LITELLM_BASE_URL = os.environ.get("LITELLM_BASE_URL", "http://litellm:4000")
PGVECTOR_HOST = os.environ.get("PGVECTOR_HOST", "pgvector")
PGVECTOR_PORT = os.environ.get("PGVECTOR_PORT", "5432")
PGVECTOR_DB = os.environ.get("PGVECTOR_DB", "bedrock_kb")
PGVECTOR_USER = os.environ.get("PGVECTOR_USER", "bedrock")
PGVECTOR_PASSWORD = os.environ.get("PGVECTOR_PASSWORD", "bedrock")

# In-memory state
_knowledge_bases: dict = {}  # kb_id -> kb metadata
_data_sources: dict = {}  # ds_id -> data source metadata
_ingestion_jobs: dict = {}  # job_id -> job metadata
_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Path routing patterns (Bedrock Agent REST API)
# ---------------------------------------------------------------------------

# PUT /knowledgebases/{kbId}/datasources/{dsId}/ingestionjobs
_RE_START_INGESTION = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/?$")
# GET /knowledgebases/{kbId}/datasources/{dsId}/ingestionjobs/{jobId}
_RE_GET_INGESTION = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/([^/]+)$")
# POST /knowledgebases/{kbId}/datasources/{dsId}/documents/get
_RE_GET_DOCUMENTS = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/get$")
# POST /knowledgebases/{kbId}/datasources/{dsId}/documents/list
_RE_LIST_DOCUMENTS = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/list$")
# POST /knowledgebases/{kbId}/datasources/{dsId}/documents/delete
_RE_DELETE_DOCUMENTS = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/delete$")


async def handle_request(method, path, headers, body, query_params):
    """Main entry point for Bedrock Agent requests."""
    # StartIngestionJob
    m = _RE_START_INGESTION.match(path)
    if m and method == "PUT":
        return await _start_ingestion_job(m.group(1), m.group(2), body)

    # GetIngestionJob
    m = _RE_GET_INGESTION.match(path)
    if m and method == "GET":
        return _get_ingestion_job(m.group(1), m.group(2), m.group(3))

    # GetKnowledgeBaseDocuments
    m = _RE_GET_DOCUMENTS.match(path)
    if m and method == "POST":
        return await _get_knowledge_base_documents(m.group(1), m.group(2), body)

    # ListKnowledgeBaseDocuments
    m = _RE_LIST_DOCUMENTS.match(path)
    if m and method == "POST":
        return await _list_knowledge_base_documents(m.group(1), m.group(2), body)

    # DeleteKnowledgeBaseDocuments
    m = _RE_DELETE_DOCUMENTS.match(path)
    if m and method == "POST":
        return await _delete_knowledge_base_documents(m.group(1), m.group(2), body)

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

async def _start_ingestion_job(kb_id: str, ds_id: str, body: bytes):
    """
    StartIngestionJob — starts asynchronous ingestion of S3 documents into pgvector.
    Creates a job record and launches a background thread to process documents.
    """
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    job_id = new_uuid()
    job = {
        "ingestionJobId": job_id,
        "knowledgeBaseId": kb_id,
        "dataSourceId": ds_id,
        "status": "STARTING",
        "startedAt": now_iso(),
        "updatedAt": now_iso(),
        "statistics": {
            "numberOfDocumentsScanned": 0,
            "numberOfDocumentsFailed": 0,
            "numberOfNewDocumentsIndexed": 0,
            "numberOfModifiedDocumentsIndexed": 0,
            "numberOfDocumentsDeleted": 0,
        },
        "description": data.get("description", ""),
    }

    with _lock:
        _ingestion_jobs[job_id] = job
        # Auto-register KB and DS if not already known
        if kb_id not in _knowledge_bases:
            _knowledge_bases[kb_id] = {
                "knowledgeBaseId": kb_id,
                "name": f"kb-{kb_id}",
                "status": "ACTIVE",
                "createdAt": now_iso(),
            }
        if ds_id not in _data_sources:
            _data_sources[ds_id] = {
                "dataSourceId": ds_id,
                "knowledgeBaseId": kb_id,
                "status": "AVAILABLE",
                "createdAt": now_iso(),
            }

    # Launch background ingestion thread
    thread = threading.Thread(
        target=_run_ingestion_job_sync,
        args=(job_id, kb_id, ds_id),
        daemon=True,
    )
    thread.start()

    return json_response({"ingestionJob": job}, 202)


def _run_ingestion_job_sync(job_id: str, kb_id: str, ds_id: str):
    """Background thread: ingest S3 objects into pgvector."""
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run_ingestion_job(job_id, kb_id, ds_id))
    except Exception as e:
        logger.error("Ingestion job %s failed: %s", job_id, e)
        with _lock:
            job = _ingestion_jobs.get(job_id, {})
            job["status"] = "FAILED"
            job["failureReasons"] = [str(e)]
            job["updatedAt"] = now_iso()
    finally:
        loop.close()


async def _run_ingestion_job(job_id: str, kb_id: str, ds_id: str):
    """Async ingestion: fetch S3 objects, generate embeddings, store in pgvector."""
    with _lock:
        job = _ingestion_jobs.get(job_id)
        if not job:
            return
        job["status"] = "IN_PROGRESS"
        job["updatedAt"] = now_iso()

    # Get S3 objects from the data source
    # Convention: data source bucket = ds_id (or configured via data source config)
    from ministack.services import s3 as s3_svc

    # Try to list objects from an S3 bucket matching the data source ID
    bucket_name = ds_id
    objects = []
    if bucket_name in s3_svc._buckets:
        for key, obj_data in s3_svc._buckets[bucket_name].get("objects", {}).items():
            objects.append((key, obj_data.get("data", b"")))

    stats = job["statistics"]
    stats["numberOfDocumentsScanned"] = len(objects)

    # Connect to pgvector
    conn = await _get_pg_connection()
    try:
        for key, data in objects:
            try:
                # Extract text content
                content = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data)
                if not content.strip():
                    stats["numberOfDocumentsFailed"] += 1
                    continue

                # Generate embedding
                embedding = await _generate_embedding(content[:8000])  # Truncate to avoid token limits

                # Insert into pgvector
                s3_uri = f"s3://{bucket_name}/{key}"
                await conn.execute(
                    """
                    INSERT INTO kb_documents (knowledge_base_id, data_source_id, s3_uri, content, metadata, embedding, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT DO NOTHING
                    """,
                    kb_id, ds_id, s3_uri, content,
                    json.dumps({"s3_key": key, "source_bucket": bucket_name}),
                    str(embedding), "INDEXED",
                )
                stats["numberOfNewDocumentsIndexed"] += 1
            except Exception as e:
                logger.warning("Failed to ingest %s/%s: %s", bucket_name, key, e)
                stats["numberOfDocumentsFailed"] += 1
    finally:
        await conn.close()

    with _lock:
        job["status"] = "COMPLETE"
        job["updatedAt"] = now_iso()
        job["completedAt"] = now_iso()


def _get_ingestion_job(kb_id: str, ds_id: str, job_id: str):
    """GetIngestionJob — returns the status and details of an ingestion job."""
    with _lock:
        job = _ingestion_jobs.get(job_id)

    if not job:
        return error_response_json("ResourceNotFoundException",
                                   f"Ingestion job {job_id} not found", 404)
    if job["knowledgeBaseId"] != kb_id or job["dataSourceId"] != ds_id:
        return error_response_json("ResourceNotFoundException",
                                   f"Ingestion job {job_id} not found for KB {kb_id} / DS {ds_id}", 404)

    return json_response({"ingestionJob": job})


async def _get_knowledge_base_documents(kb_id: str, ds_id: str, body: bytes):
    """GetKnowledgeBaseDocuments — retrieve specific documents by their identifiers."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    document_identifiers = data.get("documentIdentifiers", [])
    if not document_identifiers:
        return error_response_json("ValidationException", "documentIdentifiers is required", 400)

    try:
        conn = await _get_pg_connection()
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"pgvector is unavailable: {e}", 503)

    try:
        results = []
        for doc_id_obj in document_identifiers:
            # AWS format: {"dataSourceType": "S3", "s3": {"uri": "..."}}
            # Also accept: {"s3": {"uri": "..."}} for backwards compat
            ds_type = doc_id_obj.get("dataSourceType", "S3")
            s3_loc = doc_id_obj.get("s3", {})
            s3_uri = s3_loc.get("uri", "")
            custom_loc = doc_id_obj.get("custom", {})
            custom_id = custom_loc.get("id", "") or doc_id_obj.get("id", "")

            if ds_type == "S3" and s3_uri:
                row = await conn.fetchrow(
                    "SELECT id, s3_uri, content, metadata, status, created_at, updated_at "
                    "FROM kb_documents WHERE knowledge_base_id = $1 AND data_source_id = $2 AND s3_uri = $3",
                    kb_id, ds_id, s3_uri,
                )
            elif custom_id:
                row = await conn.fetchrow(
                    "SELECT id, s3_uri, content, metadata, status, created_at, updated_at "
                    "FROM kb_documents WHERE knowledge_base_id = $1 AND data_source_id = $2 AND id::text = $3",
                    kb_id, ds_id, custom_id,
                )
            else:
                row = None

            if row:
                results.append({
                    "documentIdentifier": {
                        "s3": {"uri": row["s3_uri"]} if row["s3_uri"] else {},
                    },
                    "status": {
                        "type": row["status"],
                    },
                    "updatedAt": row["updated_at"].isoformat() + "Z" if row["updated_at"] else now_iso(),
                })
    finally:
        await conn.close()

    return json_response({"documentDetails": results})


async def _list_knowledge_base_documents(kb_id: str, ds_id: str, body: bytes):
    """ListKnowledgeBaseDocuments — list all documents in a knowledge base data source."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    max_results = data.get("maxResults", 100)
    next_token = data.get("nextToken")

    try:
        conn = await _get_pg_connection()
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"pgvector is unavailable: {e}", 503)

    try:
        offset = int(next_token) if next_token else 0
        rows = await conn.fetch(
            "SELECT id, s3_uri, status, updated_at "
            "FROM kb_documents WHERE knowledge_base_id = $1 AND data_source_id = $2 "
            "ORDER BY created_at LIMIT $3 OFFSET $4",
            kb_id, ds_id, max_results + 1, offset,
        )
    finally:
        await conn.close()

    documents = []
    for row in rows[:max_results]:
        documents.append({
            "documentIdentifier": {
                "s3": {"uri": row["s3_uri"]} if row["s3_uri"] else {},
            },
            "status": {
                "type": row["status"],
            },
            "updatedAt": row["updated_at"].isoformat() + "Z" if row["updated_at"] else now_iso(),
        })

    result = {"documentDetails": documents}
    if len(rows) > max_results:
        result["nextToken"] = str(offset + max_results)

    return json_response(result)


async def _delete_knowledge_base_documents(kb_id: str, ds_id: str, body: bytes):
    """DeleteKnowledgeBaseDocuments — delete specific documents from the knowledge base."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    document_identifiers = data.get("documentIdentifiers", [])
    if not document_identifiers:
        return error_response_json("ValidationException", "documentIdentifiers is required", 400)

    try:
        conn = await _get_pg_connection()
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"pgvector is unavailable: {e}", 503)

    try:
        deleted = []
        for doc_id_obj in document_identifiers:
            ds_type = doc_id_obj.get("dataSourceType", "S3")
            s3_loc = doc_id_obj.get("s3", {})
            s3_uri = s3_loc.get("uri", "")
            custom_loc = doc_id_obj.get("custom", {})
            custom_id = custom_loc.get("id", "") or doc_id_obj.get("id", "")

            if ds_type == "S3" and s3_uri:
                await conn.execute(
                    "DELETE FROM kb_documents WHERE knowledge_base_id = $1 AND data_source_id = $2 AND s3_uri = $3",
                    kb_id, ds_id, s3_uri,
                )
            elif custom_id:
                await conn.execute(
                    "DELETE FROM kb_documents WHERE knowledge_base_id = $1 AND data_source_id = $2 AND id::text = $3",
                    kb_id, ds_id, custom_id,
                )
            deleted.append({
                "documentIdentifier": doc_id_obj,
                "status": {"type": "DELETED"},
            })
    finally:
        await conn.close()

    return json_response({"documentDetails": deleted})


def reset():
    """Clear all in-memory state."""
    with _lock:
        _knowledge_bases.clear()
        _data_sources.clear()
        _ingestion_jobs.clear()


def get_state():
    """Return serializable state for persistence."""
    return {
        "knowledge_bases": dict(_knowledge_bases),
        "data_sources": dict(_data_sources),
        "ingestion_jobs": dict(_ingestion_jobs),
    }
