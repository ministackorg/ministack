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

This writes to `build/mysql-plugins` by default. `Dockerfile.full` compiles and
places the tree at `/opt/ministack/mysql-plugins`; the slim image copies that
tree from the same release's full image into the same fixed runtime lookup path.
Delivery and installation happen automatically when a matching bundled artifact
exists and are silent when it is absent.

Artifact series selection uses the tag of the same resolved MySQL image that
the RDS container launch uses. Accepted version prefixes such as Aurora MySQL
`8` therefore follow the current fallback image (`mysql:8.4`) automatically.
If a custom resolved image has no recognized series tag, MiniStack warns and
leaves the plugin artifact absent while still attempting the compatibility
procedures independently.

| Engine class | Launch image source | Compatibility series | Disposition |
|---|---|---|---|
| `aurora-mysql` | Aurora version map, then `DEFAULT_AURORA_MYSQL_IMAGE` | Parsed from the selected MySQL image tag | 8.0/8.4 plugin artifacts when present; predefined S3 roles on 8.x; procedures/config always attempted. |
| `mysql` | Same MySQL version map and fallback as the launch path | Parsed from the selected MySQL image tag | Same ABI-matched artifact rule; predefined S3 roles skipped; procedures/config always attempted. |
| `mariadb` | The launch path's `mariadb:latest` selection | None | Plugin and Aurora roles skipped; RDS procedures/config still attempted independently. |

## MySQL-ready path design

The lifecycle hook runs after authenticated readiness and outside RDS store
locks. Cluster paths revalidate their container ID/epoch after the hook before
publishing readiness. It installs the local Aurora compatibility procedures on
every MySQL compute node, then installs the auth plugin when its artifact is
available. Replication cannot begin before either installation attempt.

| Compute path | Container transition | Compatibility disposition |
|---|---|---|
| First Aurora member | `_start_cluster_shared_container` (`containers.run`) then `_create_db_instance` readiness worker | Hooked before `_configure_or_defer_mysql_replication`; applies to writers and secondaries. |
| New standalone RDS instance | `_create_db_instance` (`containers.run`) readiness worker | Hooked after authenticated readiness. |
| Persisted Aurora cluster | `restore_state` through `_start_cluster_shared_container` (`containers.run`) | Hooked after authenticated readiness and before replication reconfiguration. |
| Persisted standalone instance | `restore_state` through `_start_rds_container_for_instance` (`containers.run`) | Hooked after an authenticated readiness wait. |
| Stopped Aurora cluster | `_restart_cluster_shared_container` (`container.start`) or recreate fallback, then `_start_db_cluster` readiness worker | Hooked after authenticated readiness; object-existence guards preserve grants across restart. |
| CloudFormation DBCluster/DBInstance | `cloudformation/provisioners.py` writes metadata only | No hook: this path explicitly does not create or adopt database compute. |
| Read-replica and snapshot-restore stubs | Metadata-only records | No hook: no MySQL server transitions to ready. |

The same hook creates `mysql.rds_kill`, `mysql.rds_kill_query`,
`mysql.rds_show_configuration`, and `mysql.rds_set_configuration` with binary
logging disabled for the session. The provider's Square and Cash user sets are
the source of all four procedure grants. A validation-lane sweep of every
deployed control-plane ASL definition in both regions found no `rds_%` routine
reference; the only lexical match was the unrelated `rds_managed` identifier.
The kill procedures use a root-privileged definer and real `KILL CONNECTION` /
`KILL QUERY` statements. The configuration pair models only the
binlog-retention-hours round-trip.

For Aurora MySQL 8.x, the hook also creates the two predefined roles referenced
by the Cash user set: `AWS_SELECT_S3_ACCESS` and `AWS_LOAD_S3_ACCESS`. They
intentionally carry no privileges because MiniStack does not model Aurora's S3
import/export data plane; their fidelity contract is existence so grants and
reconciliation match Aurora. The deployed-ASL sweep found no `AWS_%`
identifiers, so it adds no role beyond the two provider-source roles.
Provider-created service roles
are runtime resources and require no MiniStack fixture. Standalone MySQL never
receives these Aurora-only roles, and Aurora MySQL 5.7 skips them because its S3
integration uses privileges instead. Their configuration table and four
procedures are still installed independently. An error creating
one compatibility object is logged and does not suppress the remaining
objects.

When a stopped cluster is recreated on its retained data volume, `mysql.plugin`
can contain the prior registration even though the fresh container failed to
load the absent library at boot. The hook copies the artifact first, attempts
`UNINSTALL PLUGIN`, removes only the stale `AWSAuthenticationPlugin` row when
MySQL rejects that unload for a boot-failed plugin, reinstalls, and verifies the
plugin is `ACTIVE` before reporting success. The maintenance session disables
binary logging at entry so every current and future plugin-metadata statement
remains compute-local and cannot alter a global-cluster secondary.

Both image flavors carry the same artifacts; the slim image receives them from
the same release's full image. The existing global-replication live fixture
asserts that an IAM user created on the primary reaches the secondary without
breaking replication whenever artifacts are present. Each MySQL image-tag
update must rebuild and reload-test its series artifact.
