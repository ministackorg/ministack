"""
Shared Spark Execution Engine — K8s Job management for MiniStack.

This module is the execution backend for both EMR Containers (emr_containers.py) and
EMR EC2 (emr.py) services. When spark configuration is provided, it creates real K8s
Jobs that run spark-submit in local[*] mode inside a Spark container image. When no
spark config is present, callers fall back to mock mode (jobs immediately COMPLETED).

Each Spark job is a single K8s Job pod running spark-submit --master local[*]. There
are no separate executor pods — driver and executor threads run in one JVM process.
This is lightweight and sufficient for devbox testing.

Configuration is provided via:
  - MINISTACK_SPARK_CONFIG env var (JSON string), or
  - POST /_ministack/config with {"spark": {...}}

Config structure:
  {
    "mode": "local-k8s",
    "image": "<spark image>",
    "imagePullSecret": "ecr-registry-secret",
    "namespace": "devbox",
    "clusterSize": "small"
  }

If config is null/missing, all callers should use mock mode instead.
"""

import json
import logging
import os
import threading
import time

logger = logging.getLogger("k8s-spark")

# ---------------------------------------------------------------------------
# Spark configuration — set via env var or /_ministack/config endpoint
# ---------------------------------------------------------------------------

_spark_config: dict | None = None

# Cluster size profiles: K8s pod resource requests/limits for the single Spark pod.
# Since Spark runs in local[*] mode, these control the pod's resource budget,
# not individual executor sizing.
CLUSTER_SIZE_PROFILES = {
    "small":  {"memory": "1Gi", "cpu": "1"},
    "medium": {"memory": "2Gi", "cpu": "2"},
    "large":  {"memory": "4Gi", "cpu": "4"},
}

# Default Spark image (Salesforce production DPC 3.3 / EMR 7.9.0)
DEFAULT_SPARK_IMAGE = (
    "331455399823.dkr.ecr.us-west-2.amazonaws.com"
    "/sfci/a360/cdp-emr-eks/spark-emr-eks-7.9.0:emr-7.9-84c4181"
)
FALLBACK_SPARK_IMAGE = "apache/spark:3.5.0-java17"

# Track running jobs for status polling:
#   job_name -> {"state": ..., "stateDetails": ..., "labels": {...}, "name": ...}
# Labels are preserved so we can include virtual cluster ID, job run ID, etc.
# in EventBridge state-change events.
_job_states: dict = {}
_poller_thread: threading.Thread | None = None
_poller_stop = threading.Event()

# K8s client — lazily initialized
_k8s_batch_v1 = None
_k8s_core_v1 = None


def _init_env_config():
    """Load spark config from MINISTACK_SPARK_CONFIG env var at import time."""
    global _spark_config
    env_val = os.environ.get("MINISTACK_SPARK_CONFIG", "").strip()
    if env_val:
        try:
            _spark_config = json.loads(env_val)
            logger.info("Loaded spark config from env: mode=%s", _spark_config.get("mode"))
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in MINISTACK_SPARK_CONFIG, ignoring")


_init_env_config()


def set_spark_config(config: dict | None):
    """Set or clear the spark configuration at runtime.

    Called by the /_ministack/config endpoint when {"spark": {...}} is received.
    Starts the K8s status poller when transitioning to k8s mode.
    """
    global _spark_config
    _spark_config = config
    if config:
        logger.info("Spark config updated: mode=%s image=%s size=%s",
                     config.get("mode"), config.get("image", "default"), config.get("clusterSize", "small"))
        _ensure_poller_running()
    else:
        logger.info("Spark config cleared — reverting to mock mode")


def get_spark_config() -> dict | None:
    """Return current spark config, or None if in mock mode."""
    return _spark_config


def is_k8s_mode() -> bool:
    """Return True if spark config is present and mode is local-k8s."""
    return bool(_spark_config and _spark_config.get("mode") == "local-k8s")


# ---------------------------------------------------------------------------
# K8s client initialization (lazy, conditional import)
# ---------------------------------------------------------------------------

def _get_k8s_clients():
    """Lazily initialize K8s API clients. Imports kubernetes only when needed."""
    global _k8s_batch_v1, _k8s_core_v1
    if _k8s_batch_v1 is not None:
        return _k8s_batch_v1, _k8s_core_v1

    try:
        from kubernetes import client, config as k8s_config

        # Try in-cluster first (when running inside K8s), fall back to kubeconfig
        try:
            k8s_config.load_incluster_config()
            logger.info("Loaded in-cluster K8s config")
        except k8s_config.ConfigException:
            context = os.environ.get("MINISTACK_K8S_CONTEXT")
            k8s_config.load_kube_config(context=context)
            logger.info("Loaded kubeconfig (context=%s)", context or "default")

        _k8s_batch_v1 = client.BatchV1Api()
        _k8s_core_v1 = client.CoreV1Api()
        return _k8s_batch_v1, _k8s_core_v1
    except ImportError:
        raise RuntimeError(
            "kubernetes package not installed. Install with: pip install ministack[k8s]"
        )
    except Exception as e:
        raise RuntimeError(f"Failed to initialize K8s client: {e}")


# ---------------------------------------------------------------------------
# Job creation
# ---------------------------------------------------------------------------

def create_spark_job(
    job_name: str,
    entry_point: str,
    class_name: str = "",
    spark_args: list[str] | None = None,
    spark_conf: dict[str, str] | None = None,
    labels: dict[str, str] | None = None,
) -> str:
    """Create a K8s Job that runs spark-submit in local[*] mode.

    Args:
        job_name: Unique name for the K8s Job (e.g. "emr-jr-abc123" or "emr-step-s-XYZ")
        entry_point: Spark application JAR/py path (e.g. "s3a://bucket/app.jar" or "local:///...")
        class_name: Main class for --class argument (optional)
        spark_args: Application arguments passed after the JAR
        spark_conf: Spark configuration properties passed as --conf key=value
        labels: K8s labels for the Job metadata

    Returns:
        The job_name, for tracking.
    """
    from kubernetes import client

    batch_v1, _ = _get_k8s_clients()
    config = _spark_config or {}
    namespace = config.get("namespace", "devbox")
    image = config.get("image", DEFAULT_SPARK_IMAGE)
    pull_secret = config.get("imagePullSecret", "ecr-registry-secret")
    size = config.get("clusterSize", "small")
    resources = CLUSTER_SIZE_PROFILES.get(size, CLUSTER_SIZE_PROFILES["small"])

    # Build spark-submit args
    submit_args = []
    if class_name:
        submit_args.extend(["--class", class_name])
    submit_args.extend(["--master", "local[*]"])

    # Add --conf flags for spark properties
    if spark_conf:
        for k, v in spark_conf.items():
            submit_args.extend(["--conf", f"{k}={v}"])

    # Entry point JAR/py
    submit_args.append(entry_point)

    # Application arguments
    if spark_args:
        submit_args.extend(spark_args)

    ministack_endpoint = os.environ.get(
        "MINISTACK_INTERNAL_ENDPOINT",
        "http://ministack.devbox.svc.cluster.local:4566"
    )

    # Environment variables for the Spark pod to access MiniStack S3
    env_vars = [
        client.V1EnvVar(name="AWS_ENDPOINT_URL", value=ministack_endpoint),
        client.V1EnvVar(name="AWS_ACCESS_KEY_ID", value="test"),
        client.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="test"),
        client.V1EnvVar(name="AWS_DEFAULT_REGION", value=os.environ.get("MINISTACK_REGION", "us-east-1")),
        # S3A Hadoop filesystem config for JAR/data access via MiniStack
        client.V1EnvVar(name="SPARK_HADOOP_FS_S3A_ENDPOINT", value=ministack_endpoint),
        client.V1EnvVar(name="SPARK_HADOOP_FS_S3A_PATH_STYLE_ACCESS", value="true"),
        client.V1EnvVar(name="SPARK_HADOOP_FS_S3A_IMPL", value="org.apache.hadoop.fs.s3a.S3AFileSystem"),
    ]

    container = client.V1Container(
        name="spark",
        image=image,
        command=["/usr/lib/spark/bin/spark-submit"],
        args=submit_args,
        env=env_vars,
        resources=client.V1ResourceRequirements(
            requests={"memory": resources["memory"], "cpu": resources["cpu"]},
            limits={"memory": resources["memory"], "cpu": resources["cpu"]},
        ),
    )

    pod_spec = client.V1PodSpec(
        containers=[container],
        restart_policy="Never",
    )
    if pull_secret:
        pod_spec.image_pull_secrets = [client.V1LocalObjectReference(name=pull_secret)]

    job_labels = {"ministack/service": "spark"}
    if labels:
        job_labels.update(labels)

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace, labels=job_labels),
        spec=client.V1JobSpec(
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=job_labels),
                spec=pod_spec,
            ),
        ),
    )

    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    _job_states[job_name] = {
        "state": "PENDING",
        "stateDetails": "Job created, waiting for pod",
        "labels": labels or {},
        "name": job_name,
    }
    logger.info("Created K8s Spark Job: %s in namespace %s (image=%s, size=%s)",
                job_name, namespace, image, size)

    _ensure_poller_running()
    return job_name


# ---------------------------------------------------------------------------
# Job status
# ---------------------------------------------------------------------------

def get_job_state(job_name: str) -> dict:
    """Get the current state of a Spark job.

    Returns dict with "state" (PENDING|RUNNING|COMPLETED|FAILED|CANCELLED)
    and "stateDetails" string.
    """
    return _job_states.get(job_name, {"state": "UNKNOWN", "stateDetails": "Job not found"})


def delete_job(job_name: str) -> bool:
    """Delete/cancel a K8s Spark Job. Returns True if deleted successfully."""
    config = _spark_config or {}
    namespace = config.get("namespace", "devbox")
    try:
        from kubernetes import client
        batch_v1, _ = _get_k8s_clients()
        batch_v1.delete_namespaced_job(
            name=job_name,
            namespace=namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        )
        _job_states[job_name] = {"state": "CANCELLED", "stateDetails": "Job deleted by user"}
        logger.info("Deleted K8s Job: %s", job_name)
        return True
    except Exception as e:
        logger.warning("Failed to delete K8s Job %s: %s", job_name, e)
        return False


def _poll_job_states():
    """Background thread that polls K8s for job/pod status every 5 seconds.

    Maps K8s states to EMR-compatible states:
      Job created, pod Pending → PENDING
      Pod Running              → RUNNING
      Pod Succeeded (exit 0)   → COMPLETED
      Pod Failed               → FAILED
      Job deleted              → CANCELLED
    """
    while not _poller_stop.is_set():
        try:
            _poll_once()
        except Exception as e:
            logger.debug("Poller error (will retry): %s", e)
        _poller_stop.wait(5)


def _poll_once():
    """Single poll iteration — update states for all tracked jobs.

    When a state change is detected, emits an EventBridge event matching the
    real AWS pattern (source: aws.emr-containers or aws.emr) so that
    EventBridge rules can route notifications to SQS, just like production.
    """
    if not _spark_config:
        return

    config = _spark_config
    namespace = config.get("namespace", "devbox")

    try:
        batch_v1, core_v1 = _get_k8s_clients()
    except Exception:
        return

    for job_name in list(_job_states.keys()):
        current = _job_states[job_name]
        old_state = current["state"]
        # Skip terminal states
        if old_state in ("COMPLETED", "FAILED", "CANCELLED"):
            continue

        try:
            job = batch_v1.read_namespaced_job(name=job_name, namespace=namespace)
        except Exception:
            _update_state(job_name, "CANCELLED", "Job not found in K8s")
            continue

        status = job.status
        if status.succeeded and status.succeeded > 0:
            _update_state(job_name, "COMPLETED", "Job completed successfully")
        elif status.failed and status.failed > 0:
            _update_state(job_name, "FAILED", "Job failed")
        elif status.active and status.active > 0:
            _update_state(job_name, "RUNNING", "Spark job running")
        # else still PENDING


def _update_state(job_name: str, new_state: str, details: str):
    """Update job state and emit EventBridge event if state changed."""
    current = _job_states.get(job_name, {})
    old_state = current.get("state")
    labels = current.get("labels", {})
    current["state"] = new_state
    current["stateDetails"] = details
    _job_states[job_name] = current

    if old_state != new_state:
        logger.info("Job %s: %s → %s", job_name, old_state, new_state)
        _emit_state_change_event(job_name, new_state, details, labels)


def _emit_state_change_event(job_name: str, state: str, details: str, labels: dict):
    """Emit an EventBridge event for a job state change.

    Mimics the real AWS behavior:
      - EMR on EKS emits to source "aws.emr-containers" with detail-type "EMR Job Run State Change"
      - EMR on EC2 emits to source "aws.emr" with detail-type "EMR Step Status Change"

    The service type is determined from the job's labels (ministack/service).
    EventBridge rules configured in devbox will route these to SQS queues that
    DPC listens on.
    """
    try:
        from ministack.services import eventbridge

        service = labels.get("ministack/service", "")

        if service == "emr-containers":
            # EMR on EKS event format
            vc_id = labels.get("ministack/virtual-cluster", "")
            jr_id = labels.get("ministack/job-run", "")
            event_entry = {
                "Source": "aws.emr-containers",
                "DetailType": "EMR Job Run State Change",
                "Detail": json.dumps({
                    "severity": "INFO",
                    "name": job_name,
                    "id": jr_id,
                    "virtualClusterId": vc_id,
                    "state": state,
                    "stateDetails": details,
                }),
                "EventBusName": "default",
            }
        elif service == "emr-ec2":
            # EMR on EC2 event format
            step_id = labels.get("ministack/step", "")
            event_entry = {
                "Source": "aws.emr",
                "DetailType": "EMR Step Status Change",
                "Detail": json.dumps({
                    "severity": "INFO",
                    "stepId": step_id,
                    "name": job_name,
                    "state": state,
                    "stateDetails": details,
                }),
                "EventBusName": "default",
            }
        else:
            return  # Unknown service type, skip

        eventbridge._put_events({"Entries": [event_entry]})
        logger.debug("Emitted EventBridge event: %s %s → %s",
                     event_entry["Source"], job_name, state)

    except Exception as e:
        logger.warning("Failed to emit EventBridge event for %s: %s", job_name, e)


def _ensure_poller_running():
    """Start the background status poller if not already running."""
    global _poller_thread
    if _poller_thread and _poller_thread.is_alive():
        return
    _poller_stop.clear()
    _poller_thread = threading.Thread(target=_poll_job_states, daemon=True, name="k8s-spark-poller")
    _poller_thread.start()
    logger.info("Started K8s Spark job status poller")


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

def reset():
    """Clear all tracked job states. Called by /_ministack/reset."""
    _job_states.clear()
