"""S3-specific API endpoints for rich bucket/object browsing.

Routes:
  GET /s3/buckets                              — list all buckets with metadata
  GET /s3/buckets/{name}                       — bucket detail
  GET /s3/buckets/{name}/objects?prefix=&delimiter=  — browse objects (folder-like)
  GET /s3/buckets/{name}/objects/{key}         — single object metadata (no body)
"""

from ministack.ui.api._common import binary_response, get_query_param, json_response


async def handle(rel_path: str, query_params: dict, send):
    """Route /s3/* requests."""
    from ministack.services import s3

    parts = [p for p in rel_path.split("/") if p]

    if not parts or parts[0] != "buckets":
        await json_response(send, {"error": "Invalid S3 API path"}, status=400)
        return

    buckets = getattr(s3, "_buckets", {})
    bucket_tags = getattr(s3, "_bucket_tags", {})
    bucket_versioning = getattr(s3, "_bucket_versioning", {})
    bucket_encryption = getattr(s3, "_bucket_encryption", {})

    # GET /s3/buckets — list all buckets
    if len(parts) == 1:
        result = []
        for name, bkt in buckets.items():
            objects = bkt.get("objects", {})
            total_size = sum(obj.get("size", 0) for obj in objects.values())
            result.append({
                "name": name,
                "created": bkt.get("created", ""),
                "region": bkt.get("region") or "us-east-1",
                "object_count": len(objects),
                "total_size": total_size,
                "versioning": bucket_versioning.get(name, "Disabled"),
                "encryption": "Enabled" if name in bucket_encryption else "Disabled",
                "tags": bucket_tags.get(name, {}),
            })
        await json_response(send, {"buckets": result})
        return

    bucket_name = parts[1]
    if bucket_name not in buckets:
        await json_response(send, {"error": f"Bucket not found: {bucket_name}"}, status=404)
        return

    bkt = buckets[bucket_name]
    objects = bkt.get("objects", {})

    # GET /s3/buckets/{name}/objects/{key...} — single object metadata or download
    if len(parts) >= 3 and parts[2] == "objects" and len(parts) > 3:
        object_key = "/".join(parts[3:])
        if object_key not in objects:
            await json_response(send, {"error": f"Object not found: {object_key}"}, status=404)
            return
        obj = objects[object_key]

        # ?download=1 — serve the raw object body as a file download
        if get_query_param(query_params, "download") == "1":
            body = obj.get("body", b"")
            if isinstance(body, str):
                body = body.encode("utf-8")
            content_type = obj.get("content_type", "application/octet-stream")
            filename = object_key.rsplit("/", 1)[-1] or object_key
            await binary_response(send, body, content_type, filename)
            return

        object_tags = getattr(s3, "_object_tags", {})
        await json_response(send, {
            "bucket": bucket_name,
            "key": object_key,
            "size": obj.get("size", 0),
            "content_type": obj.get("content_type", "application/octet-stream"),
            "content_encoding": obj.get("content_encoding"),
            "etag": obj.get("etag", ""),
            "last_modified": obj.get("last_modified", ""),
            "version_id": obj.get("version_id"),
            "metadata": obj.get("metadata", {}),
            "preserved_headers": obj.get("preserved_headers", {}),
            "tags": dict(object_tags.get((bucket_name, object_key), {})),
        })
        return

    # GET /s3/buckets/{name}/objects?prefix=&delimiter= — browse objects
    if len(parts) >= 3 and parts[2] == "objects":
        prefix = get_query_param(query_params, "prefix", "")
        delimiter = get_query_param(query_params, "delimiter", "/")

        folders: set[str] = set()
        files = []
        for key, obj in objects.items():
            if not key.startswith(prefix):
                continue
            rest = key[len(prefix):]
            if delimiter and delimiter in rest:
                folder = rest[: rest.index(delimiter) + len(delimiter)]
                folders.add(prefix + folder)
            else:
                files.append({
                    "key": key,
                    "name": rest,
                    "size": obj.get("size", 0),
                    "content_type": obj.get("content_type", "application/octet-stream"),
                    "etag": obj.get("etag", ""),
                    "last_modified": obj.get("last_modified", ""),
                })

        await json_response(send, {
            "bucket": bucket_name,
            "prefix": prefix,
            "delimiter": delimiter,
            "folders": sorted(folders),
            "files": sorted(files, key=lambda f: f["key"]),
        })
        return

    # GET /s3/buckets/{name} — bucket detail
    total_size = sum(obj.get("size", 0) for obj in objects.values())
    await json_response(send, {
        "name": bucket_name,
        "created": bkt.get("created", ""),
        "region": bkt.get("region") or "us-east-1",
        "object_count": len(objects),
        "total_size": total_size,
        "versioning": bucket_versioning.get(bucket_name, "Disabled"),
        "encryption": "Enabled" if bucket_name in bucket_encryption else "Disabled",
        "tags": bucket_tags.get(bucket_name, {}),
    })
