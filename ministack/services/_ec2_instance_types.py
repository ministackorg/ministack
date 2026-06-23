"""Canonical EC2 instance-type facts for the common current-generation families.

``DescribeInstanceTypes`` / ``DescribeInstanceTypeOfferings`` feed any consumer
that filters or scores instances by capacity — IaC tools, the AWS CLI, and
cluster autoscalers. They read ``VCpuInfo.DefaultVCpus``,
``MemoryInfo.SizeInMiB``, ``ProcessorInfo.SupportedArchitectures``, and
``NetworkInfo.NetworkPerformance``; a sparse or wrong value silently scores an
instance as zero-capacity (or the wrong architecture) and drops it from the
candidate set with no error — so these must match real AWS, and a wrong value is
worse than a missing one.

``vcpus``, ``mem_mib``, and ``arch`` are the published AWS specifications for
these families (stable, documented values). ``net`` follows AWS's documented
per-size network tiers and is approximate for the largest sizes. To refresh or
extend, diff against ``aws ec2 describe-instance-types --region us-east-1
--instance-types <type>`` and lift ``VCpuInfo`` / ``MemoryInfo`` /
``ProcessorInfo`` / ``NetworkInfo``.

Scope: the common current-generation on-demand families — general purpose
(m5, m5n, m6i, m6a, m6g), compute (c5, c5n, c6i, c6a, c6g), memory (r5, r6i,
r6g), and burstable (t3, t3a). GPU, instance-store (``d``), and bare-metal
variants are intentionally out of scope; add a family here when a consumer
needs it.

Each value is ``{"vcpus": int, "mem_mib": int, "arch": str, "net": str,
"burstable": bool}`` — ``burstable`` defaults to False when omitted. ``cores`` /
``threadsPerCore`` are derived from ``arch`` at render time (x86_64 = SMT-2,
Graviton/arm64 = 1 thread/core), which matches AWS for every family here.
"""

X86 = "x86_64"
ARM = "arm64"

# Network-performance tiers (AWS documented strings); named to avoid repeating
# the exact literal on every row, not to compute it.
_UP5 = "Up to 5 Gigabit"
_UP10 = "Up to 10 Gigabit"
_UP25 = "Up to 25 Gigabit"

INSTANCE_TYPES = {
    # ── Burstable t3 / t3a — x86_64, 2 vCPUs through .large ───────────────────
    "t3.nano": {"vcpus": 2, "mem_mib": 512, "arch": X86, "net": _UP5, "burstable": True},
    "t3.micro": {"vcpus": 2, "mem_mib": 1024, "arch": X86, "net": _UP5, "burstable": True},
    "t3.small": {"vcpus": 2, "mem_mib": 2048, "arch": X86, "net": _UP5, "burstable": True},
    "t3.medium": {"vcpus": 2, "mem_mib": 4096, "arch": X86, "net": _UP5, "burstable": True},
    "t3.large": {"vcpus": 2, "mem_mib": 8192, "arch": X86, "net": _UP5, "burstable": True},
    "t3.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": X86, "net": _UP5, "burstable": True},
    "t3.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.nano": {"vcpus": 2, "mem_mib": 512, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.micro": {"vcpus": 2, "mem_mib": 1024, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.small": {"vcpus": 2, "mem_mib": 2048, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.medium": {"vcpus": 2, "mem_mib": 4096, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.large": {"vcpus": 2, "mem_mib": 8192, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": X86, "net": _UP5, "burstable": True},
    "t3a.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": X86, "net": _UP5, "burstable": True},

    # ── General purpose m5 — x86_64, 4 GiB/vCPU ──────────────────────────────
    "m5.large": {"vcpus": 2, "mem_mib": 8192, "arch": X86, "net": _UP10},
    "m5.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "m5.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "m5.4xlarge": {"vcpus": 16, "mem_mib": 65536, "arch": X86, "net": _UP10},
    "m5.8xlarge": {"vcpus": 32, "mem_mib": 131072, "arch": X86, "net": "10 Gigabit"},
    "m5.12xlarge": {"vcpus": 48, "mem_mib": 196608, "arch": X86, "net": "12 Gigabit"},
    "m5.16xlarge": {"vcpus": 64, "mem_mib": 262144, "arch": X86, "net": "20 Gigabit"},

    # ── General purpose m6i — x86_64, 4 GiB/vCPU ─────────────────────────────
    "m6i.large": {"vcpus": 2, "mem_mib": 8192, "arch": X86, "net": _UP10},
    "m6i.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "m6i.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "m6i.4xlarge": {"vcpus": 16, "mem_mib": 65536, "arch": X86, "net": _UP10},
    "m6i.8xlarge": {"vcpus": 32, "mem_mib": 131072, "arch": X86, "net": "12.5 Gigabit"},
    "m6i.12xlarge": {"vcpus": 48, "mem_mib": 196608, "arch": X86, "net": "18.75 Gigabit"},
    "m6i.16xlarge": {"vcpus": 64, "mem_mib": 262144, "arch": X86, "net": "25 Gigabit"},

    # ── General purpose m6a — x86_64, 4 GiB/vCPU ─────────────────────────────
    "m6a.large": {"vcpus": 2, "mem_mib": 8192, "arch": X86, "net": _UP10},
    "m6a.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "m6a.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "m6a.4xlarge": {"vcpus": 16, "mem_mib": 65536, "arch": X86, "net": _UP10},
    "m6a.8xlarge": {"vcpus": 32, "mem_mib": 131072, "arch": X86, "net": "12.5 Gigabit"},
    "m6a.12xlarge": {"vcpus": 48, "mem_mib": 196608, "arch": X86, "net": "18.75 Gigabit"},
    "m6a.16xlarge": {"vcpus": 64, "mem_mib": 262144, "arch": X86, "net": "25 Gigabit"},

    # ── General purpose m5n — x86_64, 4 GiB/vCPU, enhanced network ────────────
    "m5n.large": {"vcpus": 2, "mem_mib": 8192, "arch": X86, "net": _UP25},
    "m5n.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": X86, "net": _UP25},
    "m5n.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": X86, "net": _UP25},
    "m5n.4xlarge": {"vcpus": 16, "mem_mib": 65536, "arch": X86, "net": _UP25},
    "m5n.8xlarge": {"vcpus": 32, "mem_mib": 131072, "arch": X86, "net": "25 Gigabit"},
    "m5n.12xlarge": {"vcpus": 48, "mem_mib": 196608, "arch": X86, "net": "50 Gigabit"},
    "m5n.16xlarge": {"vcpus": 64, "mem_mib": 262144, "arch": X86, "net": "75 Gigabit"},

    # ── General purpose m6g — Graviton2 (arm64), 4 GiB/vCPU ───────────────────
    "m6g.large": {"vcpus": 2, "mem_mib": 8192, "arch": ARM, "net": _UP10},
    "m6g.xlarge": {"vcpus": 4, "mem_mib": 16384, "arch": ARM, "net": _UP10},
    "m6g.2xlarge": {"vcpus": 8, "mem_mib": 32768, "arch": ARM, "net": _UP10},
    "m6g.4xlarge": {"vcpus": 16, "mem_mib": 65536, "arch": ARM, "net": _UP10},
    "m6g.8xlarge": {"vcpus": 32, "mem_mib": 131072, "arch": ARM, "net": "12 Gigabit"},
    "m6g.12xlarge": {"vcpus": 48, "mem_mib": 196608, "arch": ARM, "net": "20 Gigabit"},
    "m6g.16xlarge": {"vcpus": 64, "mem_mib": 262144, "arch": ARM, "net": "25 Gigabit"},

    # ── Compute optimized c6i — x86_64, 2 GiB/vCPU ───────────────────────────
    "c6i.large": {"vcpus": 2, "mem_mib": 4096, "arch": X86, "net": _UP10},
    "c6i.xlarge": {"vcpus": 4, "mem_mib": 8192, "arch": X86, "net": _UP10},
    "c6i.2xlarge": {"vcpus": 8, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "c6i.4xlarge": {"vcpus": 16, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "c6i.8xlarge": {"vcpus": 32, "mem_mib": 65536, "arch": X86, "net": "12.5 Gigabit"},
    "c6i.12xlarge": {"vcpus": 48, "mem_mib": 98304, "arch": X86, "net": "18.75 Gigabit"},
    "c6i.16xlarge": {"vcpus": 64, "mem_mib": 131072, "arch": X86, "net": "25 Gigabit"},

    # ── Compute optimized c6a — x86_64, 2 GiB/vCPU ───────────────────────────
    "c6a.large": {"vcpus": 2, "mem_mib": 4096, "arch": X86, "net": _UP10},
    "c6a.xlarge": {"vcpus": 4, "mem_mib": 8192, "arch": X86, "net": _UP10},
    "c6a.2xlarge": {"vcpus": 8, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "c6a.4xlarge": {"vcpus": 16, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "c6a.8xlarge": {"vcpus": 32, "mem_mib": 65536, "arch": X86, "net": "12.5 Gigabit"},
    "c6a.12xlarge": {"vcpus": 48, "mem_mib": 98304, "arch": X86, "net": "18.75 Gigabit"},
    "c6a.16xlarge": {"vcpus": 64, "mem_mib": 131072, "arch": X86, "net": "25 Gigabit"},

    # ── Compute optimized c5 — x86_64, 2 GiB/vCPU (irregular 9xl/18xl sizes) ──
    "c5.large": {"vcpus": 2, "mem_mib": 4096, "arch": X86, "net": _UP10},
    "c5.xlarge": {"vcpus": 4, "mem_mib": 8192, "arch": X86, "net": _UP10},
    "c5.2xlarge": {"vcpus": 8, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "c5.4xlarge": {"vcpus": 16, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "c5.9xlarge": {"vcpus": 36, "mem_mib": 73728, "arch": X86, "net": "10 Gigabit"},
    "c5.12xlarge": {"vcpus": 48, "mem_mib": 98304, "arch": X86, "net": "12 Gigabit"},
    "c5.18xlarge": {"vcpus": 72, "mem_mib": 147456, "arch": X86, "net": "25 Gigabit"},

    # ── Compute optimized c5n — x86_64, ~5.25 GiB/vCPU base (curated memory) ──
    "c5n.large": {"vcpus": 2, "mem_mib": 5376, "arch": X86, "net": _UP25},
    "c5n.xlarge": {"vcpus": 4, "mem_mib": 10752, "arch": X86, "net": _UP25},
    "c5n.2xlarge": {"vcpus": 8, "mem_mib": 21504, "arch": X86, "net": _UP25},
    "c5n.4xlarge": {"vcpus": 16, "mem_mib": 43008, "arch": X86, "net": _UP25},
    "c5n.9xlarge": {"vcpus": 36, "mem_mib": 98304, "arch": X86, "net": "50 Gigabit"},
    "c5n.18xlarge": {"vcpus": 72, "mem_mib": 196608, "arch": X86, "net": "100 Gigabit"},

    # ── Compute optimized c6g — Graviton2 (arm64), 2 GiB/vCPU ─────────────────
    "c6g.large": {"vcpus": 2, "mem_mib": 4096, "arch": ARM, "net": _UP10},
    "c6g.xlarge": {"vcpus": 4, "mem_mib": 8192, "arch": ARM, "net": _UP10},
    "c6g.2xlarge": {"vcpus": 8, "mem_mib": 16384, "arch": ARM, "net": _UP10},
    "c6g.4xlarge": {"vcpus": 16, "mem_mib": 32768, "arch": ARM, "net": _UP10},
    "c6g.8xlarge": {"vcpus": 32, "mem_mib": 65536, "arch": ARM, "net": "12 Gigabit"},
    "c6g.12xlarge": {"vcpus": 48, "mem_mib": 98304, "arch": ARM, "net": "20 Gigabit"},
    "c6g.16xlarge": {"vcpus": 64, "mem_mib": 131072, "arch": ARM, "net": "25 Gigabit"},

    # ── Memory optimized r5 — x86_64, 8 GiB/vCPU ─────────────────────────────
    "r5.large": {"vcpus": 2, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "r5.xlarge": {"vcpus": 4, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "r5.2xlarge": {"vcpus": 8, "mem_mib": 65536, "arch": X86, "net": _UP10},
    "r5.4xlarge": {"vcpus": 16, "mem_mib": 131072, "arch": X86, "net": _UP10},
    "r5.8xlarge": {"vcpus": 32, "mem_mib": 262144, "arch": X86, "net": "10 Gigabit"},
    "r5.12xlarge": {"vcpus": 48, "mem_mib": 393216, "arch": X86, "net": "12 Gigabit"},
    "r5.16xlarge": {"vcpus": 64, "mem_mib": 524288, "arch": X86, "net": "20 Gigabit"},

    # ── Memory optimized r6i — x86_64, 8 GiB/vCPU ────────────────────────────
    "r6i.large": {"vcpus": 2, "mem_mib": 16384, "arch": X86, "net": _UP10},
    "r6i.xlarge": {"vcpus": 4, "mem_mib": 32768, "arch": X86, "net": _UP10},
    "r6i.2xlarge": {"vcpus": 8, "mem_mib": 65536, "arch": X86, "net": _UP10},
    "r6i.4xlarge": {"vcpus": 16, "mem_mib": 131072, "arch": X86, "net": _UP10},
    "r6i.8xlarge": {"vcpus": 32, "mem_mib": 262144, "arch": X86, "net": "12.5 Gigabit"},
    "r6i.12xlarge": {"vcpus": 48, "mem_mib": 393216, "arch": X86, "net": "18.75 Gigabit"},
    "r6i.16xlarge": {"vcpus": 64, "mem_mib": 524288, "arch": X86, "net": "25 Gigabit"},

    # ── Memory optimized r6g — Graviton2 (arm64), 8 GiB/vCPU ──────────────────
    "r6g.large": {"vcpus": 2, "mem_mib": 16384, "arch": ARM, "net": _UP10},
    "r6g.xlarge": {"vcpus": 4, "mem_mib": 32768, "arch": ARM, "net": _UP10},
    "r6g.2xlarge": {"vcpus": 8, "mem_mib": 65536, "arch": ARM, "net": _UP10},
    "r6g.4xlarge": {"vcpus": 16, "mem_mib": 131072, "arch": ARM, "net": _UP10},
    "r6g.8xlarge": {"vcpus": 32, "mem_mib": 262144, "arch": ARM, "net": "12 Gigabit"},
    "r6g.12xlarge": {"vcpus": 48, "mem_mib": 393216, "arch": ARM, "net": "20 Gigabit"},
    "r6g.16xlarge": {"vcpus": 64, "mem_mib": 524288, "arch": ARM, "net": "25 Gigabit"},
}


def cores_and_threads(vcpus, arch):
    """Derive (defaultCores, defaultThreadsPerCore).

    Graviton (arm64) runs 1 thread/core; x86_64 families here use SMT-2. Holds
    for every family in this table.
    """
    if arch == ARM:
        return vcpus, 1
    return vcpus // 2, 2
