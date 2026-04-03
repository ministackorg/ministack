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
_knowledge_bases: dict = {}
_data_sources: dict = {}
_ingestion_jobs: dict = {}
_agents: dict = {}  # agent_id -> agent metadata
_agent_aliases: dict = {}  # f"{agent_id}:{alias_id}" -> alias metadata
_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Path routing patterns (Bedrock Agent REST API)
# ---------------------------------------------------------------------------

# Knowledge Base CRUD
_RE_KB_ID = re.compile(r"^/knowledgebases/([^/]+)$")
_RE_KBS = re.compile(r"^/knowledgebases/?$")

# Data Source CRUD
_RE_DS_ID = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)$")
_RE_DSS = re.compile(r"^/knowledgebases/([^/]+)/datasources/?$")

# Ingestion jobs
_RE_START_INGESTION = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/?$")
_RE_GET_INGESTION = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/([^/]+)$")
_RE_STOP_INGESTION = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/([^/]+)/stop$")

# Agent CRUD
_RE_AGENT_ID = re.compile(r"^/agents/([^/]+?)/?$")
_RE_AGENTS = re.compile(r"^/agents/?$")
_RE_AGENT_ALIAS_ID = re.compile(r"^/agents/([^/]+)/agentaliases/([^/]+?)/?$")
_RE_AGENT_ALIASES = re.compile(r"^/agents/([^/]+)/agentaliases/?$")

# Document operations
_RE_GET_DOCUMENTS = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/getDocuments$")
_RE_LIST_DOCUMENTS = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/?$")
_RE_DELETE_DOCUMENTS = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/deleteDocuments$")


async def handle_request(method, path, headers, body, query_params):
    """Main entry point for Bedrock Agent requests."""

    # --- Stop ingestion (must match before GET ingestion) ---
    m = _RE_STOP_INGESTION.match(path)
    if m and method == "POST":
        return _stop_ingestion_job(m.group(1), m.group(2), m.group(3))

    # --- Ingestion jobs (most specific paths first) ---
    m = _RE_GET_INGESTION.match(path)
    if m and method == "GET":
        return _get_ingestion_job(m.group(1), m.group(2), m.group(3))

    m = _RE_START_INGESTION.match(path)
    if m and method == "PUT":
        return await _start_ingestion_job(m.group(1), m.group(2), body)
    if m and method == "POST":
        return _list_ingestion_jobs(m.group(1), m.group(2), body)

    # --- Document operations ---
    m = _RE_GET_DOCUMENTS.match(path)
    if m and method == "POST":
        return await _get_knowledge_base_documents(m.group(1), m.group(2), body)

    m = _RE_DELETE_DOCUMENTS.match(path)
    if m and method == "POST":
        return await _delete_knowledge_base_documents(m.group(1), m.group(2), body)

    m = _RE_LIST_DOCUMENTS.match(path)
    if m and method == "POST":
        return await _list_knowledge_base_documents(m.group(1), m.group(2), body)

    # --- Data Source CRUD ---
    m = _RE_DS_ID.match(path)
    if m:
        kb_id, ds_id = m.group(1), m.group(2)
        if method == "GET":
            return _get_data_source(kb_id, ds_id)
        elif method == "PUT":
            return _update_data_source(kb_id, ds_id, body)
        elif method == "DELETE":
            return _delete_data_source(kb_id, ds_id)

    m = _RE_DSS.match(path)
    if m:
        kb_id = m.group(1)
        if method == "PUT":
            return _create_data_source(kb_id, body)
        elif method == "POST":
            return _list_data_sources(kb_id, body)

    # --- Agent Aliases ---
    m = _RE_AGENT_ALIAS_ID.match(path)
    if m:
        agent_id, alias_id = m.group(1), m.group(2)
        if method == "GET":
            return _get_agent_alias(agent_id, alias_id)
        elif method == "PUT":
            return _update_agent_alias(agent_id, alias_id, body)
        elif method == "DELETE":
            return _delete_agent_alias(agent_id, alias_id)

    m = _RE_AGENT_ALIASES.match(path)
    if m:
        agent_id = m.group(1)
        if method == "PUT":
            return _create_agent_alias(agent_id, body)
        elif method == "POST":
            return _list_agent_aliases(agent_id, body)

    # --- Agent CRUD ---
    m = _RE_AGENT_ID.match(path)
    if m:
        agent_id = m.group(1)
        if method == "GET":
            return _get_agent(agent_id)
        elif method == "PUT":
            return _update_agent(agent_id, body)
        elif method == "POST":
            return _prepare_agent(agent_id)
        elif method == "DELETE":
            return _delete_agent(agent_id)

    if _RE_AGENTS.match(path):
        if method == "PUT":
            return _create_agent(body)
        elif method == "POST":
            return _list_agents(body)

    # --- Knowledge Base CRUD ---
    m = _RE_KB_ID.match(path)
    if m:
        kb_id = m.group(1)
        if method == "GET":
            return _get_knowledge_base(kb_id)
        elif method == "PUT":
            return _update_knowledge_base(kb_id, body)
        elif method == "DELETE":
            return _delete_knowledge_base(kb_id)

    m = _RE_KBS.match(path)
    if m:
        if method == "PUT":
            return _create_knowledge_base(body)
        elif method == "POST":
            return _list_knowledge_bases(body)

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
        _now = now_iso()
        if kb_id not in _knowledge_bases:
            _knowledge_bases[kb_id] = {
                "knowledgeBaseId": kb_id,
                "knowledgeBaseArn": f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:knowledge-base/{kb_id}",
                "name": f"kb-{kb_id}",
                "status": "ACTIVE",
                "createdAt": _now,
                "updatedAt": _now,
            }
        if ds_id not in _data_sources:
            _data_sources[ds_id] = {
                "dataSourceId": ds_id,
                "knowledgeBaseId": kb_id,
                "name": f"ds-{ds_id}",
                "status": "AVAILABLE",
                "createdAt": _now,
                "updatedAt": _now,
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
    from ministack.services import s3 as s3_svc

    # Resolve bucket name: check data source config first, then fall back to ds_id
    bucket_name = ds_id
    with _lock:
        ds_meta = _data_sources.get(ds_id, {})
    ds_config = ds_meta.get("dataSourceConfiguration", {})
    s3_config = ds_config.get("s3Configuration", {})
    bucket_arn = s3_config.get("bucketArn", "")
    if bucket_arn:
        # Extract bucket name from ARN: arn:aws:s3:::my-bucket -> my-bucket
        bucket_name = bucket_arn.split(":::")[-1] if ":::" in bucket_arn else bucket_arn

    objects = []
    if bucket_name in s3_svc._buckets:
        for key, obj_data in s3_svc._buckets[bucket_name].get("objects", {}).items():
            objects.append((key, obj_data.get("body", b"")))

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


def _stop_ingestion_job(kb_id: str, ds_id: str, job_id: str):
    """StopIngestionJob — stop a running ingestion job."""
    with _lock:
        job = _ingestion_jobs.get(job_id)
        if not job or job["knowledgeBaseId"] != kb_id or job["dataSourceId"] != ds_id:
            return error_response_json("ResourceNotFoundException",
                                       f"Ingestion job {job_id} not found", 404)
        if job["status"] in ("COMPLETE", "FAILED", "STOPPED"):
            return error_response_json("ConflictException",
                                       f"Job {job_id} is already {job['status']}", 409)
        job["status"] = "STOPPED"
        job["updatedAt"] = now_iso()

    return json_response({"ingestionJob": job}, 202)


def _list_ingestion_jobs(kb_id: str, ds_id: str, body: bytes):
    """ListIngestionJobs — list all ingestion jobs for a knowledge base data source."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    max_results = data.get("maxResults", 100)

    with _lock:
        jobs = [j for j in _ingestion_jobs.values()
                if j.get("knowledgeBaseId") == kb_id and j.get("dataSourceId") == ds_id]

    summaries = [{
        "ingestionJobId": j["ingestionJobId"],
        "knowledgeBaseId": j["knowledgeBaseId"],
        "dataSourceId": j["dataSourceId"],
        "status": j["status"],
        "startedAt": j.get("startedAt", ""),
        "updatedAt": j.get("updatedAt", ""),
        "statistics": j.get("statistics", {}),
        "description": j.get("description", ""),
    } for j in jobs[:max_results]]

    result = {"ingestionJobSummaries": summaries}
    if len(jobs) > max_results:
        result["nextToken"] = str(max_results)
    return json_response(result)


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


# ---------------------------------------------------------------------------
# Knowledge Base CRUD
# ---------------------------------------------------------------------------

def _create_knowledge_base(body: bytes):
    """CreateKnowledgeBase — create a new knowledge base."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    name = data.get("name", "")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    kb_id = new_uuid()[:10].upper()
    now = now_iso()
    kb = {
        "knowledgeBaseId": kb_id,
        "knowledgeBaseArn": f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:knowledge-base/{kb_id}",
        "name": name,
        "description": data.get("description", ""),
        "roleArn": data.get("roleArn", f"arn:aws:iam::{ACCOUNT_ID}:role/bedrock-kb-role"),
        "knowledgeBaseConfiguration": data.get("knowledgeBaseConfiguration", {
            "type": "VECTOR", "vectorKnowledgeBaseConfiguration": {"embeddingModelArn": ""}
        }),
        "storageConfiguration": data.get("storageConfiguration", {}),
        "status": "ACTIVE",
        "createdAt": now,
        "updatedAt": now,
    }

    with _lock:
        _knowledge_bases[kb_id] = kb

    return json_response({"knowledgeBase": kb}, 202)


def _get_knowledge_base(kb_id: str):
    """GetKnowledgeBase — return knowledge base metadata."""
    with _lock:
        kb = _knowledge_bases.get(kb_id)
    if not kb:
        return error_response_json("ResourceNotFoundException",
                                   f"Knowledge base {kb_id} not found", 404)
    return json_response({"knowledgeBase": kb})


def _list_knowledge_bases(body: bytes):
    """ListKnowledgeBases — list all knowledge bases."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    max_results = data.get("maxResults", 100)

    with _lock:
        items = list(_knowledge_bases.values())

    summaries = [{
        "knowledgeBaseId": kb["knowledgeBaseId"],
        "name": kb["name"],
        "description": kb.get("description", ""),
        "status": kb["status"],
        "updatedAt": kb.get("updatedAt") or kb.get("createdAt") or now_iso(),
    } for kb in items[:max_results]]

    result = {"knowledgeBaseSummaries": summaries}
    if len(items) > max_results:
        result["nextToken"] = str(max_results)
    return json_response(result)


def _delete_knowledge_base(kb_id: str):
    """DeleteKnowledgeBase — delete a knowledge base."""
    with _lock:
        if kb_id not in _knowledge_bases:
            return error_response_json("ResourceNotFoundException",
                                       f"Knowledge base {kb_id} not found", 404)
        del _knowledge_bases[kb_id]
        # Also remove associated data sources
        to_remove = [ds_id for ds_id, ds in _data_sources.items()
                     if ds.get("knowledgeBaseId") == kb_id]
        for ds_id in to_remove:
            del _data_sources[ds_id]

    return json_response({"knowledgeBaseId": kb_id, "status": "DELETING"}, 202)


# ---------------------------------------------------------------------------
# Data Source CRUD
# ---------------------------------------------------------------------------

def _create_data_source(kb_id: str, body: bytes):
    """CreateDataSource — create a new data source for a knowledge base."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    with _lock:
        if kb_id not in _knowledge_bases:
            # Auto-create KB if not found (matches existing ingestion behavior)
            _knowledge_bases[kb_id] = {
                "knowledgeBaseId": kb_id, "name": f"kb-{kb_id}",
                "status": "ACTIVE", "createdAt": now_iso(),
            }

    name = data.get("name", "")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    ds_id = new_uuid()[:10].upper()
    now = now_iso()
    ds_config = data.get("dataSourceConfiguration", {})

    ds = {
        "dataSourceId": ds_id,
        "knowledgeBaseId": kb_id,
        "name": name,
        "description": data.get("description", ""),
        "status": "AVAILABLE",
        "dataSourceConfiguration": ds_config,
        "dataDeletionPolicy": data.get("dataDeletionPolicy", "RETAIN"),
        "vectorIngestionConfiguration": data.get("vectorIngestionConfiguration", {}),
        "createdAt": now,
        "updatedAt": now,
    }

    with _lock:
        _data_sources[ds_id] = ds

    return json_response({"dataSource": ds})


def _get_data_source(kb_id: str, ds_id: str):
    """GetDataSource — return data source metadata."""
    with _lock:
        ds = _data_sources.get(ds_id)
    if not ds or ds.get("knowledgeBaseId") != kb_id:
        return error_response_json("ResourceNotFoundException",
                                   f"Data source {ds_id} not found in KB {kb_id}", 404)
    return json_response({"dataSource": ds})


def _list_data_sources(kb_id: str, body: bytes):
    """ListDataSources — list all data sources for a knowledge base."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    max_results = data.get("maxResults", 100)

    with _lock:
        items = [ds for ds in _data_sources.values()
                 if ds.get("knowledgeBaseId") == kb_id]

    summaries = [{
        "dataSourceId": ds["dataSourceId"],
        "knowledgeBaseId": ds["knowledgeBaseId"],
        "name": ds["name"],
        "description": ds.get("description", ""),
        "status": ds["status"],
        "updatedAt": ds.get("updatedAt") or ds.get("createdAt") or now_iso(),
    } for ds in items[:max_results]]

    result = {"dataSourceSummaries": summaries}
    if len(items) > max_results:
        result["nextToken"] = str(max_results)
    return json_response(result)


def _update_knowledge_base(kb_id: str, body: bytes):
    """UpdateKnowledgeBase — update a knowledge base."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    with _lock:
        kb = _knowledge_bases.get(kb_id)
        if not kb:
            return error_response_json("ResourceNotFoundException",
                                       f"Knowledge base {kb_id} not found", 404)
        for field in ("name", "description", "roleArn"):
            if field in data:
                kb[field] = data[field]
        if "knowledgeBaseConfiguration" in data:
            kb["knowledgeBaseConfiguration"] = data["knowledgeBaseConfiguration"]
        if "storageConfiguration" in data:
            kb["storageConfiguration"] = data["storageConfiguration"]
        kb["updatedAt"] = now_iso()

    return json_response({"knowledgeBase": kb}, 202)


def _update_data_source(kb_id: str, ds_id: str, body: bytes):
    """UpdateDataSource — update a data source."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    with _lock:
        ds = _data_sources.get(ds_id)
        if not ds or ds.get("knowledgeBaseId") != kb_id:
            return error_response_json("ResourceNotFoundException",
                                       f"Data source {ds_id} not found in KB {kb_id}", 404)
        for field in ("name", "description", "dataDeletionPolicy"):
            if field in data:
                ds[field] = data[field]
        if "dataSourceConfiguration" in data:
            ds["dataSourceConfiguration"] = data["dataSourceConfiguration"]
        if "vectorIngestionConfiguration" in data:
            ds["vectorIngestionConfiguration"] = data["vectorIngestionConfiguration"]
        ds["updatedAt"] = now_iso()

    return json_response({"dataSource": ds})


def _delete_data_source(kb_id: str, ds_id: str):
    """DeleteDataSource — delete a data source."""
    with _lock:
        ds = _data_sources.get(ds_id)
        if not ds or ds.get("knowledgeBaseId") != kb_id:
            return error_response_json("ResourceNotFoundException",
                                       f"Data source {ds_id} not found in KB {kb_id}", 404)
        del _data_sources[ds_id]

    return json_response({
        "dataSourceId": ds_id,
        "knowledgeBaseId": kb_id,
        "status": "DELETING",
    }, 202)


# ---------------------------------------------------------------------------
# Agent CRUD
# ---------------------------------------------------------------------------

def _create_agent(body: bytes):
    """CreateAgent."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    name = data.get("agentName", "")
    if not name:
        return error_response_json("ValidationException", "agentName is required", 400)

    agent_id = new_uuid()[:10].upper()
    now = now_iso()
    agent = {
        "agentId": agent_id,
        "agentArn": f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:agent/{agent_id}",
        "agentName": name,
        "description": data.get("description", ""),
        "agentStatus": "NOT_PREPARED",
        "agentVersion": "DRAFT",
        "foundationModel": data.get("foundationModel", ""),
        "instruction": data.get("instruction", ""),
        "agentResourceRoleArn": data.get("agentResourceRoleArn", f"arn:aws:iam::{ACCOUNT_ID}:role/bedrock-agent"),
        "idleSessionTTLInSeconds": data.get("idleSessionTTLInSeconds", 600),
        "createdAt": now,
        "updatedAt": now,
    }

    with _lock:
        _agents[agent_id] = agent

    return json_response({"agent": agent}, 202)


def _get_agent(agent_id: str):
    """GetAgent."""
    with _lock:
        agent = _agents.get(agent_id)
    if not agent:
        return error_response_json("ResourceNotFoundException",
                                   f"Agent {agent_id} not found", 404)
    return json_response({"agent": agent})


def _list_agents(body: bytes):
    """ListAgents."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}
    max_results = data.get("maxResults", 100)

    with _lock:
        items = list(_agents.values())

    summaries = [{
        "agentId": a["agentId"],
        "agentName": a["agentName"],
        "agentStatus": a["agentStatus"],
        "description": a.get("description", ""),
        "updatedAt": a.get("updatedAt", a.get("createdAt", "")),
    } for a in items[:max_results]]

    result = {"agentSummaries": summaries}
    if len(items) > max_results:
        result["nextToken"] = str(max_results)
    return json_response(result)


def _update_agent(agent_id: str, body: bytes):
    """UpdateAgent."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    with _lock:
        agent = _agents.get(agent_id)
        if not agent:
            return error_response_json("ResourceNotFoundException",
                                       f"Agent {agent_id} not found", 404)
        for field in ("agentName", "description", "foundationModel", "instruction",
                      "agentResourceRoleArn", "idleSessionTTLInSeconds"):
            if field in data:
                agent[field] = data[field]
        agent["updatedAt"] = now_iso()
        agent["agentStatus"] = "NOT_PREPARED"

    return json_response({"agent": agent}, 202)


def _prepare_agent(agent_id: str):
    """PrepareAgent — mark agent as PREPARED."""
    with _lock:
        agent = _agents.get(agent_id)
        if not agent:
            return error_response_json("ResourceNotFoundException",
                                       f"Agent {agent_id} not found", 404)
        agent["agentStatus"] = "PREPARED"
        agent["preparedAt"] = now_iso()
        agent["updatedAt"] = now_iso()

    return json_response({
        "agentId": agent_id,
        "agentStatus": "PREPARED",
        "agentVersion": "DRAFT",
        "preparedAt": agent["preparedAt"],
    }, 202)


def _delete_agent(agent_id: str):
    """DeleteAgent."""
    with _lock:
        if agent_id not in _agents:
            return error_response_json("ResourceNotFoundException",
                                       f"Agent {agent_id} not found", 404)
        del _agents[agent_id]
        # Remove aliases
        to_del = [k for k in _agent_aliases if k.startswith(f"{agent_id}:")]
        for k in to_del:
            del _agent_aliases[k]

    return json_response({"agentId": agent_id, "agentStatus": "DELETING"}, 202)


# ---------------------------------------------------------------------------
# Agent Alias CRUD
# ---------------------------------------------------------------------------

def _create_agent_alias(agent_id: str, body: bytes):
    """CreateAgentAlias."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    with _lock:
        if agent_id not in _agents:
            return error_response_json("ResourceNotFoundException",
                                       f"Agent {agent_id} not found", 404)

    name = data.get("agentAliasName", "")
    if not name:
        return error_response_json("ValidationException", "agentAliasName required", 400)

    alias_id = new_uuid()[:10].upper()
    now = now_iso()
    alias = {
        "agentAliasId": alias_id,
        "agentAliasArn": f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:agent-alias/{agent_id}/{alias_id}",
        "agentAliasName": name,
        "agentId": agent_id,
        "agentAliasStatus": "PREPARED",
        "description": data.get("description", ""),
        "routingConfiguration": data.get("routingConfiguration", [{"agentVersion": "DRAFT"}]),
        "createdAt": now,
        "updatedAt": now,
    }

    with _lock:
        _agent_aliases[f"{agent_id}:{alias_id}"] = alias

    return json_response({"agentAlias": alias}, 202)


def _get_agent_alias(agent_id: str, alias_id: str):
    """GetAgentAlias."""
    with _lock:
        alias = _agent_aliases.get(f"{agent_id}:{alias_id}")
    if not alias:
        return error_response_json("ResourceNotFoundException",
                                   f"Alias {alias_id} not found for agent {agent_id}", 404)
    return json_response({"agentAlias": alias})


def _list_agent_aliases(agent_id: str, body: bytes):
    """ListAgentAliases."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}
    max_results = data.get("maxResults", 100)

    with _lock:
        items = [a for k, a in _agent_aliases.items() if k.startswith(f"{agent_id}:")]

    summaries = [{
        "agentAliasId": a["agentAliasId"],
        "agentAliasName": a["agentAliasName"],
        "agentAliasStatus": a["agentAliasStatus"],
        "description": a.get("description", ""),
        "createdAt": a["createdAt"],
        "updatedAt": a["updatedAt"],
    } for a in items[:max_results]]

    result = {"agentAliasSummaries": summaries}
    if len(items) > max_results:
        result["nextToken"] = str(max_results)
    return json_response(result)


def _update_agent_alias(agent_id: str, alias_id: str, body: bytes):
    """UpdateAgentAlias."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    with _lock:
        alias = _agent_aliases.get(f"{agent_id}:{alias_id}")
        if not alias:
            return error_response_json("ResourceNotFoundException",
                                       f"Alias {alias_id} not found", 404)
        for field in ("agentAliasName", "description", "routingConfiguration"):
            if field in data:
                alias[field] = data[field]
        alias["updatedAt"] = now_iso()

    return json_response({"agentAlias": alias}, 202)


def _delete_agent_alias(agent_id: str, alias_id: str):
    """DeleteAgentAlias."""
    key = f"{agent_id}:{alias_id}"
    with _lock:
        if key not in _agent_aliases:
            return error_response_json("ResourceNotFoundException",
                                       f"Alias {alias_id} not found", 404)
        del _agent_aliases[key]
    return json_response({"agentAliasId": alias_id, "agentAliasStatus": "DELETING", "agentId": agent_id}, 202)


def reset():
    """Clear all in-memory state."""
    with _lock:
        _knowledge_bases.clear()
        _data_sources.clear()
        _ingestion_jobs.clear()
        _agents.clear()
        _agent_aliases.clear()


def get_state():
    """Return serializable state for persistence."""
    return {
        "knowledge_bases": dict(_knowledge_bases),
        "data_sources": dict(_data_sources),
        "ingestion_jobs": dict(_ingestion_jobs),
    }
