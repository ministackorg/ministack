"""
AWS Cloud Map (Service Discovery) emulator.
JSON-based API via X-Amz-Target: Route53AutoNaming_v20170314.
"""

import copy
import json
import logging
import os
import time
import xml.etree.ElementTree as ET

from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)
from ministack.services import route53

logger = logging.getLogger("servicediscovery")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state
_namespaces = AccountScopedDict()     # ns_id -> namespace dict
_services = AccountScopedDict()       # svc_id -> service dict
_instances = AccountScopedDict()      # svc_id -> {instance_id -> instance dict}
_operations = AccountScopedDict()     # op_id -> operation dict
_resource_tags = AccountScopedDict()  # resource_arn -> [{"Key": ..., "Value": ...}]
_service_attributes = AccountScopedDict()      # svc_id -> {key: value}
_instance_health_status = AccountScopedDict()  # svc_id -> {instance_id: status}
_instances_revision = AccountScopedDict()      # svc_id -> int


def get_state():
    return {
        "namespaces": copy.deepcopy(_namespaces),
        "services": copy.deepcopy(_services),
        "instances": copy.deepcopy(_instances),
        "operations": copy.deepcopy(_operations),
        "resource_tags": copy.deepcopy(_resource_tags),
        "service_attributes": copy.deepcopy(_service_attributes),
        "instance_health_status": copy.deepcopy(_instance_health_status),
        "instances_revision": copy.deepcopy(_instances_revision),
    }


def load_persisted_state(data):
    if not data:
        return
    _namespaces.update(data.get("namespaces", {}))
    _services.update(data.get("services", {}))
    _instances.update(data.get("instances", {}))
    _operations.update(data.get("operations", {}))
    _resource_tags.update(data.get("resource_tags", {}))
    _service_attributes.update(data.get("service_attributes", {}))
    _instance_health_status.update(data.get("instance_health_status", {}))
    _instances_revision.update(data.get("instances_revision", {}))


def reset():
    _namespaces.clear()
    _services.clear()
    _instances.clear()
    _operations.clear()
    _resource_tags.clear()
    _service_attributes.clear()
    _instance_health_status.clear()
    _instances_revision.clear()


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    # Keep action in payload for handlers that need context
    data["_action"] = action

    handlers = {
        "CreateHttpNamespace": _create_namespace,
        "CreatePrivateDnsNamespace": _create_namespace,
        "CreatePublicDnsNamespace": _create_namespace,
        "CreateService": _create_service,
        "DeleteNamespace": _delete_namespace,
        "DeleteService": _delete_service,
        "DeleteServiceAttributes": _delete_service_attributes,
        "DeregisterInstance": _deregister_instance,
        "DiscoverInstances": _discover_instances,
        "DiscoverInstancesRevision": _discover_instances_revision,
        "GetInstance": _get_instance,
        "GetInstancesHealthStatus": _get_instances_health_status,
        "GetNamespace": _get_namespace,
        "GetOperation": _get_operation,
        "GetService": _get_service,
        "GetServiceAttributes": _get_service_attributes,
        "ListInstances": _list_instances,
        "ListNamespaces": _list_namespaces,
        "ListOperations": _list_operations,
        "ListServices": _list_services,
        "ListTagsForResource": _list_tags_for_resource,
        "RegisterInstance": _register_instance,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "UpdateHttpNamespace": _update_namespace,
        "UpdateInstanceCustomHealthStatus": _update_instance_custom_health_status,
        "UpdatePrivateDnsNamespace": _update_namespace,
        "UpdatePublicDnsNamespace": _update_namespace,
        "UpdateService": _update_service,
        "UpdateServiceAttributes": _update_service_attributes,
    }

    handler = handlers.get(action)
    if not handler:
        logger.warning("Unsupported Cloud Map action: %s", action)
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)

    import inspect

    if inspect.iscoroutinefunction(handler):
        return await handler(data)
    return handler(data)


def _namespace_arn(ns_id: str) -> str:
    return f"arn:aws:servicediscovery:{get_region()}:{get_account_id()}:namespace/{ns_id}"


def _service_arn(svc_id: str) -> str:
    return f"arn:aws:servicediscovery:{get_region()}:{get_account_id()}:service/{svc_id}"


def _create_operation(op_type: str, targets=None):
    op_id = new_uuid()
    targets = targets or {}

    now = int(time.time())
    op = {
        "Id": op_id,
        "Status": "SUCCESS",
        "Type": op_type,
        "Targets": targets,
        "CreateDate": now,
        "UpdateDate": now,
    }
    _operations[op_id] = op
    return op_id


def _touch_instances_revision(service_id: str):
    _instances_revision[service_id] = int(_instances_revision.get(service_id, 0)) + 1


async def _create_namespace(data):
    ns_name = data.get("Name")
    if not ns_name:
        return error_response_json("InvalidInput", "Name is required", 400)
    for existing in _namespaces.values():
        if existing["Name"] == ns_name:
            return error_response_json("NamespaceAlreadyExists", f"Namespace {ns_name} already exists", 409)

    ns_id = f"ns-{new_uuid()[:8]}"
    action = data.get("_action", "")

    # Infer namespace type from action; fallback uses request fields.
    is_private = action == "CreatePrivateDnsNamespace" or "Vpc" in data
    is_public = action == "CreatePublicDnsNamespace"
    is_http = action == "CreateHttpNamespace"
    if not action:
        if "Vpc" in data:
            is_private = True
        elif data.get("DnsConfig"):
            is_public = True
        else:
            lowered = ns_name.lower()
            if "http" in lowered:
                is_http = True
            elif "private" in lowered:
                is_private = True
            else:
                is_public = True

    ns_type = "DNS_PUBLIC"
    if is_private:
        ns_type = "DNS_PRIVATE"
    elif is_http:
        ns_type = "HTTP"

    namespace = {
        "Id": ns_id,
        "Arn": _namespace_arn(ns_id),
        "Name": ns_name,
        "Type": ns_type,
        "Description": data.get("Description"),
        "CreateDate": int(time.time()),
    }

    if ns_type != "HTTP":
        zone_name = ns_name if ns_name.endswith(".") else ns_name + "."
        xml_body = f"""<CreateHostedZoneRequest xmlns=\"https://route53.amazonaws.com/doc/2013-04-01/\">
            <Name>{zone_name}</Name>
            <CallerReference>{new_uuid()}</CallerReference>
            <HostedZoneConfig>
                <Comment>Created by Cloud Map</Comment>
                <PrivateZone>{"true" if is_private else "false"}</PrivateZone>
            </HostedZoneConfig>
        </CreateHostedZoneRequest>"""

        status, _, body = await route53.handle_request(
            "POST",
            "/2013-04-01/hostedzone",
            {},
            xml_body.encode("utf-8"),
            {},
        )
        if status >= 300:
            return error_response_json("InternalFailure", "Failed to create Route53 hosted zone", 500)

        root = ET.fromstring(body)
        zone_id_el = root.find(".//{https://route53.amazonaws.com/doc/2013-04-01/}Id")
        if zone_id_el is None or not zone_id_el.text:
            return error_response_json("InternalFailure", "Hosted zone ID missing in Route53 response", 500)
        zone_id = zone_id_el.text.split("/")[-1]

        namespace["Properties"] = {
            "DnsProperties": {
                "HostedZoneId": zone_id,
            }
        }
    else:
        namespace["Properties"] = {
            "HttpProperties": {
                "HttpName": ns_name,
            }
        }

    _namespaces[ns_id] = namespace
    tags = data.get("Tags", [])
    if tags:
        _resource_tags[namespace["Arn"]] = tags

    op_id = _create_operation("CREATE_NAMESPACE", {"NAMESPACE": ns_id})
    return json_response({"OperationId": op_id})


def _delete_namespace(data):
    ns_id = data.get("Id")
    if not ns_id:
        return error_response_json("InvalidInput", "Id is required", 400)
    if ns_id not in _namespaces:
        return error_response_json("NamespaceNotFound", "Namespace not found", 404)

    namespace = _namespaces.pop(ns_id)
    _resource_tags.pop(namespace.get("Arn", ""), None)

    op_id = _create_operation("DELETE_NAMESPACE", {"NAMESPACE": ns_id})
    return json_response({"OperationId": op_id})


def _get_namespace(data):
    ns_id = data.get("Id")
    ns = _namespaces.get(ns_id)
    if not ns:
        return error_response_json("NamespaceNotFound", "Namespace not found", 404)
    return json_response({"Namespace": ns})


def _list_namespaces(data):
    return json_response({"Namespaces": list(_namespaces.values())})


def _create_service(data):
    ns_id = data.get("NamespaceId")
    if not ns_id:
        return error_response_json("InvalidInput", "NamespaceId is required", 400)
    if ns_id not in _namespaces:
        return error_response_json("NamespaceNotFound", "Namespace not found", 404)

    name = data.get("Name")
    if not name:
        return error_response_json("InvalidInput", "Name is required", 400)

    svc_id = f"srv-{new_uuid()[:8]}"
    service = {
        "Id": svc_id,
        "Arn": _service_arn(svc_id),
        "Name": name,
        "NamespaceId": ns_id,
        "Description": data.get("Description"),
        "DnsConfig": data.get("DnsConfig"),
        "HealthCheckConfig": data.get("HealthCheckConfig"),
        "HealthCheckCustomConfig": data.get("HealthCheckCustomConfig"),
        "CreateDate": int(time.time()),
    }
    _services[svc_id] = service
    _service_attributes.setdefault(svc_id, {})
    _instance_health_status.setdefault(svc_id, {})
    _instances_revision.setdefault(svc_id, 1)

    tags = data.get("Tags", [])
    if tags:
        _resource_tags[service["Arn"]] = tags

    return json_response({"Service": service})


def _delete_service(data):
    svc_id = data.get("Id")
    if not svc_id:
        return error_response_json("InvalidInput", "Id is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    service = _services.pop(svc_id)
    _instances.pop(svc_id, None)
    _service_attributes.pop(svc_id, None)
    _instance_health_status.pop(svc_id, None)
    _instances_revision.pop(svc_id, None)
    _resource_tags.pop(service.get("Arn", ""), None)
    return json_response({})


def _delete_service_attributes(data):
    svc_id = data.get("ServiceId")
    attributes = data.get("Attributes", [])
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    current = _service_attributes.setdefault(svc_id, {})
    for key in attributes:
        current.pop(key, None)
    return json_response({})


def _get_service(data):
    svc_id = data.get("Id")
    svc = _services.get(svc_id)
    if not svc:
        return error_response_json("ServiceNotFound", "Service not found", 404)
    return json_response({"Service": svc})


def _list_services(data):
    return json_response({"Services": list(_services.values())})


def _register_instance(data):
    svc_id = data.get("ServiceId")
    inst_id = data.get("InstanceId")
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if not inst_id:
        return error_response_json("InvalidInput", "InstanceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    _instances.setdefault(svc_id, {})[inst_id] = {
        "Id": inst_id,
        "Attributes": data.get("Attributes", {}),
    }
    _instance_health_status.setdefault(svc_id, {})[inst_id] = "HEALTHY"
    _touch_instances_revision(svc_id)

    op_id = _create_operation("REGISTER_INSTANCE", {"INSTANCE": inst_id, "SERVICE": svc_id})
    return json_response({"OperationId": op_id})


def _deregister_instance(data):
    svc_id = data.get("ServiceId")
    inst_id = data.get("InstanceId")
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if not inst_id:
        return error_response_json("InvalidInput", "InstanceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    if svc_id in _instances and inst_id in _instances[svc_id]:
        del _instances[svc_id][inst_id]
    if svc_id in _instance_health_status and inst_id in _instance_health_status[svc_id]:
        del _instance_health_status[svc_id][inst_id]
    _touch_instances_revision(svc_id)

    op_id = _create_operation("DEREGISTER_INSTANCE", {"INSTANCE": inst_id, "SERVICE": svc_id})
    return json_response({"OperationId": op_id})


def _get_instance(data):
    svc_id = data.get("ServiceId")
    inst_id = data.get("InstanceId")
    inst = _instances.get(svc_id, {}).get(inst_id)
    if not inst:
        return error_response_json("InstanceNotFound", "Instance not found", 404)
    return json_response({"Instance": inst})


def _list_instances(data):
    svc_id = data.get("ServiceId")
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)
    return json_response({"Instances": list(_instances.get(svc_id, {}).values())})


def _discover_instances(data):
    ns_name = data.get("NamespaceName")
    svc_name = data.get("ServiceName")
    if not ns_name or not svc_name:
        return error_response_json("InvalidInput", "NamespaceName and ServiceName are required", 400)

    namespace = next((n for n in _namespaces.values() if n.get("Name") == ns_name), None)
    if not namespace:
        return error_response_json("NamespaceNotFound", "Namespace not found", 404)

    service = next(
        (
            s
            for s in _services.values()
            if s.get("Name") == svc_name and s.get("NamespaceId") == namespace.get("Id")
        ),
        None,
    )
    if not service:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    out = []
    requested_health_status = data.get("HealthStatus", "")
    for inst in _instances.get(service["Id"], {}).values():
        health = _instance_health_status.get(service["Id"], {}).get(inst["Id"], "HEALTHY")
        if requested_health_status and requested_health_status != "ALL" and health != requested_health_status:
            continue
        out.append(
            {
                "InstanceId": inst["Id"],
                "NamespaceName": ns_name,
                "ServiceName": svc_name,
                "Attributes": inst.get("Attributes", {}),
                "HealthStatus": health,
            }
        )
    return json_response({"Instances": out})


def _discover_instances_revision(data):
    ns_name = data.get("NamespaceName")
    svc_name = data.get("ServiceName")
    if not ns_name or not svc_name:
        return error_response_json("InvalidInput", "NamespaceName and ServiceName are required", 400)

    namespace = next((n for n in _namespaces.values() if n.get("Name") == ns_name), None)
    if not namespace:
        return error_response_json("NamespaceNotFound", "Namespace not found", 404)

    service = next(
        (
            s
            for s in _services.values()
            if s.get("Name") == svc_name and s.get("NamespaceId") == namespace.get("Id")
        ),
        None,
    )
    if not service:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    return json_response({"InstancesRevision": int(_instances_revision.get(service["Id"], 1))})


def _get_instances_health_status(data):
    svc_id = data.get("ServiceId")
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    statuses = _instance_health_status.get(svc_id, {})
    filtered_instances = data.get("Instances") or list(statuses.keys())
    filtered = {iid: statuses.get(iid, "UNKNOWN") for iid in filtered_instances}

    max_results = int(data.get("MaxResults", len(filtered) or 1))
    next_token = data.get("NextToken")
    start = int(next_token) if str(next_token).isdigit() else 0
    items = list(filtered.items())
    page = items[start:start + max_results]
    out = {k: v for k, v in page}

    result = {"Status": out}
    if start + max_results < len(items):
        result["NextToken"] = str(start + max_results)
    return json_response(result)


def _get_operation(data):
    op_id = data.get("OperationId")
    op = _operations.get(op_id)
    if not op:
        return error_response_json("OperationNotFound", "Operation not found", 404)
    return json_response({"Operation": op})


def _list_operations(data):
    ops = list(_operations.values())

    for f in data.get("Filters", []) or []:
        name = f.get("Name")
        values = set(f.get("Values", []))
        if not name or not values:
            continue
        if name == "STATUS":
            ops = [o for o in ops if o.get("Status") in values]
        elif name == "TYPE":
            ops = [o for o in ops if o.get("Type") in values]
        elif name == "NAMESPACE_ID":
            ops = [o for o in ops if o.get("Targets", {}).get("NAMESPACE") in values]
        elif name == "SERVICE_ID":
            ops = [o for o in ops if o.get("Targets", {}).get("SERVICE") in values]

    max_results = int(data.get("MaxResults", len(ops) or 1))
    next_token = data.get("NextToken")
    start = int(next_token) if str(next_token).isdigit() else 0
    page = ops[start:start + max_results]

    result = {"Operations": page}
    if start + max_results < len(ops):
        result["NextToken"] = str(start + max_results)
    return json_response(result)


def _get_service_attributes(data):
    svc_id = data.get("ServiceId")
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)
    return json_response(
        {
            "ServiceAttributes": {
                "ServiceArn": _service_arn(svc_id),
                "ResourceOwner": "SELF",
                "Attributes": _service_attributes.get(svc_id, {}),
            }
        }
    )


def _update_service_attributes(data):
    svc_id = data.get("ServiceId")
    attrs = data.get("Attributes", {})
    if not svc_id:
        return error_response_json("InvalidInput", "ServiceId is required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    current = _service_attributes.setdefault(svc_id, {})
    current.update(attrs)
    return json_response({})


def _update_namespace(data):
    ns_id = data.get("Id")
    if not ns_id:
        return error_response_json("InvalidInput", "Id is required", 400)
    ns = _namespaces.get(ns_id)
    if not ns:
        return error_response_json("NamespaceNotFound", "Namespace not found", 404)

    namespace_update = data.get("Namespace", {})
    if "Description" in namespace_update:
        ns["Description"] = namespace_update.get("Description")

    op_id = _create_operation("UPDATE_NAMESPACE", {"NAMESPACE": ns_id})
    return json_response({"OperationId": op_id})


def _update_instance_custom_health_status(data):
    svc_id = data.get("ServiceId")
    inst_id = data.get("InstanceId")
    status = data.get("Status")

    if not svc_id or not inst_id or not status:
        return error_response_json("InvalidInput", "ServiceId, InstanceId, and Status are required", 400)
    if svc_id not in _services:
        return error_response_json("ServiceNotFound", "Service not found", 404)
    if inst_id not in _instances.get(svc_id, {}):
        return error_response_json("InstanceNotFound", "Instance not found", 404)

    _instance_health_status.setdefault(svc_id, {})[inst_id] = status
    _touch_instances_revision(svc_id)
    return 200, {"Content-Type": "application/x-amz-json-1.0"}, b""


def _update_service(data):
    svc_id = data.get("Id")
    if not svc_id:
        return error_response_json("InvalidInput", "Id is required", 400)
    svc = _services.get(svc_id)
    if not svc:
        return error_response_json("ServiceNotFound", "Service not found", 404)

    update = data.get("Service", {})
    if "Description" in update:
        svc["Description"] = update.get("Description")
    if "DnsConfig" in update:
        svc["DnsConfig"] = update.get("DnsConfig")
    if "HealthCheckConfig" in update:
        svc["HealthCheckConfig"] = update.get("HealthCheckConfig")
    if "HealthCheckCustomConfig" in update:
        svc["HealthCheckCustomConfig"] = update.get("HealthCheckCustomConfig")

    op_id = _create_operation("UPDATE_SERVICE", {"SERVICE": svc_id})
    return json_response({"OperationId": op_id})


def _tag_resource(data):
    arn = data.get("ResourceARN")
    if not arn:
        return error_response_json("InvalidInput", "ResourceARN is required", 400)

    incoming = data.get("Tags", [])
    existing = {t.get("Key"): t for t in _resource_tags.get(arn, []) if t.get("Key")}
    for tag in incoming:
        key = tag.get("Key")
        if key:
            existing[key] = {"Key": key, "Value": tag.get("Value", "")}
    _resource_tags[arn] = list(existing.values())
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN")
    if not arn:
        return error_response_json("InvalidInput", "ResourceARN is required", 400)

    keys = set(data.get("TagKeys", []))
    if arn in _resource_tags:
        _resource_tags[arn] = [t for t in _resource_tags[arn] if t.get("Key") not in keys]
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN")
    if not arn:
        return error_response_json("InvalidInput", "ResourceARN is required", 400)
    return json_response({"Tags": _resource_tags.get(arn, [])})
