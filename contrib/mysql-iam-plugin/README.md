# MiniStack MySQL IAM authentication plugin

This directory contains MiniStack's server-side compatibility plugin for
Aurora MySQL users declared with `AWSAuthenticationPlugin`.

The initial L0 implementation deliberately rejects every login, matching
MySQL's `mysql_no_login` behavior. It supports provider workflows that create,
alter, grant, revoke, inspect, and drop IAM-authenticated users without
pretending that local IAM token validation exists.

The same C++ source is compiled separately against MySQL 8.0 and 8.4 headers.
MySQL checks the authentication-plugin interface version when the library is
loaded, so artifacts must stay separated by series and architecture:

```text
<root>/<series>/<arch>/aws_auth_plugin.so
```

Supported v1 combinations are MySQL 8.0 and 8.4 on `amd64` and `arm64`.
Rebuild and test the artifacts when the corresponding MySQL image tag moves;
within-series server plugin ABI stability is not an explicit MySQL guarantee.
The official runtime images expose only a minimal repository. Installing
`mysql-community-devel` from the full repository conflicts with files already
owned by `mysql-community-server-minimal`, and it does not contain the server
plugin header. The build therefore installs the exact-version
`mysql-community-debugsource` package from the image's series repository and
compiles with its server headers. Generated `mysql_version.h` and the unused
server-internal headers are excluded with MySQL's `MYSQL_ABI_CHECK` compile
mode. `MYSQL_DYNAMIC_PLUGIN` emits the three loader-facing symbols instead of
the built-in-plugin symbol names, and the image build asserts that the interface
version symbol is exported. MySQL's interface version check remains the
load-time ABI guard.

Build the 8.0 and 8.4 artifacts for the host architecture with Docker from the
repository root:

```bash
contrib/mysql-iam-plugin/build.sh
```

This writes to `build/mysql-plugins` by default. Point source-mode MiniStack at
that directory with `MINISTACK_MYSQL_IAM_PLUGIN_DIR`; the full Docker image
places the same tree at `/opt/ministack/mysql-plugins`. Set
`MINISTACK_MYSQL_IAM_AUTH=off` to disable delivery and installation. The
default `auto` mode is silent when no matching artifact exists. Unknown mode
values emit one warning per process and are treated as `auto`; names reserved
for later fidelity levels do not change the L0 reject-all behavior.

## MySQL-ready path design

The lifecycle hook runs after authenticated readiness and outside RDS store
locks. Cluster paths revalidate their container ID/epoch after the hook before
publishing readiness. This preserves the existing artifact-absent path and
ensures replication cannot begin before the plugin installation attempt.

| Compute path | Container transition | IAM plugin disposition |
|---|---|---|
| First Aurora member | `_start_cluster_shared_container` (`containers.run`) then `_create_db_instance` readiness worker | Hooked before `_configure_or_defer_mysql_replication`; applies to writers and secondaries. |
| New standalone RDS instance | `_create_db_instance` (`containers.run`) readiness worker | Hooked after authenticated readiness. |
| Persisted Aurora cluster | `restore_state` through `_start_cluster_shared_container` (`containers.run`) | Hooked after authenticated readiness and before replication reconfiguration. |
| Persisted standalone instance | `restore_state` through `_start_rds_container_for_instance` (`containers.run`) | Hooked with an artifact-gated readiness wait; no wait or other behavior change when the artifact is absent or disabled. |
| Stopped Aurora cluster | `_restart_cluster_shared_container` (`container.start`) or recreate fallback, then `_start_db_cluster` readiness worker | Hooked after authenticated readiness; the idempotence guard handles preserved installations. |
| CloudFormation DBCluster/DBInstance | `cloudformation/provisioners.py` writes metadata only | No hook: this path explicitly does not create or adopt database compute. |
| Read-replica and snapshot-restore stubs | Metadata-only records | No hook: no MySQL server transitions to ready. |

Residuals: the full image carries the artifacts; the slim image remains
artifact-free and therefore auto-off. The existing global-replication live
fixture now asserts that an IAM user created on the primary reaches the
secondary without breaking replication whenever artifacts are present. Each
MySQL image-tag update must rebuild and reload-test its series artifact.
