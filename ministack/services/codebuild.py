"""
CodeBuild Service Emulator.
JSON-based API via X-Amz-Target: CodeBuild_20161006.<Operation>.

Supports:
  Projects:  CreateProject, BatchGetProjects, ListProjects,
             UpdateProject, DeleteProject
  Builds:    StartBuild, BatchGetBuilds, StopBuild,
             ListBuilds, ListBuildsForProject, BatchDeleteBuilds
"""

import copy
import json
import logging
import os
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import error_response_json, json_response, new_uuid, now_iso

logger = logging.getLogger("codebuild")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_projects: dict = {}    # project_name -> project record
_builds: dict = {}      # build_id -> build record


def reset():
    _projects.clear()
    _builds.clear()


def get_state():
    return copy.deepcopy({
        "projects": _projects,
        "builds": _builds,
    })


def restore_state(data):
    _projects.update(data.get("projects", {}))
    _builds.update(data.get("builds", {}))


_restored = load_state("codebuild")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _project_arn(name):
    return f"arn:aws:codebuild:{REGION}:{ACCOUNT_ID}:project/{name}"


def _build_arn(build_id):
    return f"arn:aws:codebuild:{REGION}:{ACCOUNT_ID}:build/{build_id}"


def _build_id(project_name):
    """Generate a build ID like 'project-name:build-uuid'."""
    return f"{project_name}:{new_uuid()}"


def _make_build_record(project, build_id, source_version=None):
    """Create a build record that immediately shows SUCCEEDED."""
    now = now_iso()
    return {
        "id": build_id,
        "arn": _build_arn(build_id),
        "buildNumber": len([b for b in _builds.values() if b["projectName"] == project["name"]]) + 1,
        "startTime": now,
        "endTime": now,
        "currentPhase": "COMPLETED",
        "buildStatus": "SUCCEEDED",
        "sourceVersion": source_version or project.get("sourceVersion", "refs/heads/main"),
        "projectName": project["name"],
        "phases": [
            {"phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "PROVISIONING", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "DOWNLOAD_SOURCE", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "INSTALL", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "PRE_BUILD", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "BUILD", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "POST_BUILD", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "UPLOAD_ARTIFACTS", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "FINALIZING", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "COMPLETED", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
        ],
        "source": project.get("source", {}),
        "artifacts": project.get("artifacts", {"type": "NO_ARTIFACTS"}),
        "environment": project.get("environment", {}),
        "logs": {
            "groupName": f"/aws/codebuild/{project['name']}",
            "streamName": build_id.replace(":", "/"),
        },
        "timeoutInMinutes": project.get("timeoutInMinutes", 60),
        "initiator": f"{ACCOUNT_ID}/user",
        "encryptionKey": f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:alias/aws/codebuild",
    }


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateProject": _create_project,
        "BatchGetProjects": _batch_get_projects,
        "ListProjects": _list_projects,
        "UpdateProject": _update_project,
        "DeleteProject": _delete_project,
        "StartBuild": _start_build,
        "BatchGetBuilds": _batch_get_builds,
        "StopBuild": _stop_build,
        "ListBuilds": _list_builds,
        "ListBuildsForProject": _list_builds_for_project,
        "BatchDeleteBuilds": _batch_delete_builds,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Project handlers
# ---------------------------------------------------------------------------

def _create_project(data):
    name = data.get("name", "")
    if not name:
        return error_response_json("InvalidInputException", "Project name is required", 400)
    if name in _projects:
        return error_response_json("ResourceAlreadyExistsException",
                                   f"Project already exists: {name}", 400)

    now = now_iso()
    project = {
        "name": name,
        "arn": _project_arn(name),
        "description": data.get("description", ""),
        "source": data.get("source", {"type": "NO_SOURCE"}),
        "sourceVersion": data.get("sourceVersion", ""),
        "artifacts": data.get("artifacts", {"type": "NO_ARTIFACTS"}),
        "environment": data.get("environment", {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        }),
        "serviceRole": data.get("serviceRole",
                                f"arn:aws:iam::{ACCOUNT_ID}:role/codebuild-role"),
        "timeoutInMinutes": data.get("timeoutInMinutes", 60),
        "tags": data.get("tags", []),
        "created": now,
        "lastModified": now,
        "encryptionKey": data.get("encryptionKey",
                                  f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:alias/aws/codebuild"),
        "badge": {"badgeEnabled": False},
    }
    _projects[name] = project
    logger.info("CreateProject: %s", name)
    return json_response({"project": _project_shape(project)})


def _project_shape(project):
    """Return the project dict in the shape boto3 expects."""
    return {
        "name": project["name"],
        "arn": project["arn"],
        "description": project.get("description", ""),
        "source": project.get("source", {}),
        "sourceVersion": project.get("sourceVersion", ""),
        "artifacts": project.get("artifacts", {}),
        "environment": project.get("environment", {}),
        "serviceRole": project.get("serviceRole", ""),
        "timeoutInMinutes": project.get("timeoutInMinutes", 60),
        "tags": project.get("tags", []),
        "created": project["created"],
        "lastModified": project["lastModified"],
        "encryptionKey": project.get("encryptionKey", ""),
        "badge": project.get("badge", {}),
    }


def _batch_get_projects(data):
    names = data.get("names", [])
    found = []
    not_found = []
    for name in names:
        project = _projects.get(name)
        if project:
            found.append(_project_shape(project))
        else:
            not_found.append(name)
    return json_response({"projects": found, "projectsNotFound": not_found})


def _list_projects(data):
    sort_by = data.get("sortBy", "NAME")
    sort_order = data.get("sortOrder", "ASCENDING")
    names = list(_projects.keys())
    if sort_by == "NAME":
        names.sort(reverse=(sort_order == "DESCENDING"))
    elif sort_by == "LAST_MODIFIED_TIME":
        names.sort(key=lambda n: _projects[n].get("lastModified", ""),
                   reverse=(sort_order == "DESCENDING"))
    return json_response({"projects": names})


def _update_project(data):
    name = data.get("name", "")
    if not name or name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {name}", 400)
    project = _projects[name]
    for key in ("description", "source", "sourceVersion", "artifacts",
                "environment", "serviceRole", "timeoutInMinutes", "tags",
                "encryptionKey"):
        if key in data:
            project[key] = data[key]
    project["lastModified"] = now_iso()
    logger.info("UpdateProject: %s", name)
    return json_response({"project": _project_shape(project)})


def _delete_project(data):
    name = data.get("name", "")
    if not name or name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {name}", 400)
    del _projects[name]
    logger.info("DeleteProject: %s", name)
    return json_response({})


# ---------------------------------------------------------------------------
# Build handlers
# ---------------------------------------------------------------------------

def _start_build(data):
    project_name = data.get("projectName", "")
    if not project_name or project_name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {project_name}", 400)
    project = _projects[project_name]
    bid = _build_id(project_name)
    build = _make_build_record(project, bid, data.get("sourceVersion"))
    _builds[bid] = build
    logger.info("StartBuild: %s -> %s", project_name, bid)
    return json_response({"build": build})


def _batch_get_builds(data):
    ids = data.get("ids", [])
    found = []
    not_found = []
    for bid in ids:
        build = _builds.get(bid)
        if build:
            found.append(build)
        else:
            not_found.append(bid)
    return json_response({"builds": found, "buildsNotFound": not_found})


def _stop_build(data):
    bid = data.get("id", "")
    build = _builds.get(bid)
    if not build:
        return error_response_json("ResourceNotFoundException",
                                   f"Build not found: {bid}", 400)
    build["buildStatus"] = "STOPPED"
    build["endTime"] = now_iso()
    build["currentPhase"] = "COMPLETED"
    logger.info("StopBuild: %s", bid)
    return json_response({"build": build})


def _list_builds(data):
    sort_order = data.get("sortOrder", "DESCENDING")
    ids = list(_builds.keys())
    ids.sort(key=lambda bid: _builds[bid].get("startTime", ""),
             reverse=(sort_order == "DESCENDING"))
    return json_response({"ids": ids})


def _list_builds_for_project(data):
    project_name = data.get("projectName", "")
    if not project_name or project_name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {project_name}", 400)
    sort_order = data.get("sortOrder", "DESCENDING")
    ids = [bid for bid, b in _builds.items() if b["projectName"] == project_name]
    ids.sort(key=lambda bid: _builds[bid].get("startTime", ""),
             reverse=(sort_order == "DESCENDING"))
    return json_response({"ids": ids})


def _batch_delete_builds(data):
    ids = data.get("ids", [])
    deleted = []
    not_deleted = []
    for bid in ids:
        if bid in _builds:
            del _builds[bid]
            deleted.append(bid)
        else:
            not_deleted.append(bid)
    return json_response({"buildsDeleted": deleted, "buildsNotDeleted": not_deleted})
