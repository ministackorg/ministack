# MiniStack MySQL IAM authentication plugin

This directory contains MiniStack's server-side compatibility plugin for
Aurora MySQL users declared with `AWSAuthenticationPlugin`.

The initial L0 implementation deliberately rejects every login, matching
MySQL's `mysql_no_login` behavior. It supports provider workflows that create,
alter, grant, revoke, inspect, and drop IAM-authenticated users without
enabling direct IAM logins before the isolated proxy topology is provisioned.

## Stage 7: inactive Python gatekeeper

`ministack/core/mysqlproxy.py` implements the narrow MySQL connection adapter.
It relays ordinary password authentication to MySQL and delegates IAM admission
to a caller-supplied Python authorizer. No C++ HTTP callback or libcurl dependency
is needed. The opt-in connection fixture uses the existing `rds_iam._decision`
implementation with a resource capability and SDK-signed tokens; it does not
duplicate SigV4 or IAM policy evaluation.

`aws_auth_plugin.cc` is the single plugin implementation. Its minimal accepting
path is enabled by `-DMINISTACK_IAM_PROXY_AUTH=1` only in the isolated live test
fixture. This compile-time switch defaults to zero; it is not a runtime setting
or a second AUTH flag. **Normal builds do not enable acceptance.**
`Dockerfile.full` continues to build the default reject-all mode from that source.
The accepting variant must never replace that artifact while MySQL is directly
reachable. A caller inside the trusted backend namespace can bypass the proxy;
the tests demonstrate both that bypass and denial from outside the namespace.

MySQL selects the private `ministack_iam_gate_v1` client method for IAM accounts.
The adapter rejects client claims of that method and translates the backend's
auth switch to `mysql_clear_password`. Account creation and changes between IAM
and password methods therefore require no cached user list. MySQL still owns
account locks, SQL identity, grants, and password verification.

The SSLRequest and subsequent login must declare identical capabilities and
character sets. Usernames are restricted to 1–32 printable, non-space ASCII
characters without single quotes, with handshake collation IDs 8, 33, 45, 46 or
255. Other forms fail closed instead of risking a different identity after
MySQL's charset conversion, quote removal or truncation. Broader username and
encoding support is deferred; these restrictions also apply to password users.

With `AUTH=true`, IAM admission requires frontend TLS before requesting a token
and applies the existing token/policy decision. False or unset remains permissive
for IAM credentials and transport. Valid resource capability, current binding,
and resource IAM enablement remain required by the authorizer in either mode.
Password users retain MySQL's password checks and account-specific SSL rules.

### Validation and limits

Offline handshake tests run in the normal Python test lane without image builds:

```sh
uv run --extra dev pytest tests/test_mysqlproxy.py tests/test_rds_iam.py tests/test_rds_iam_plugin.py -q
```

The opt-in live tests compile both modes of the original plugin and connect through the Python
adapter to MySQL 8.0 and 8.4. They cover signed-token acceptance/rejection,
password users, account changes, grants, TLS rules, method spoofing, disabled
LOCAL INFILE, and backend isolation. They are not part of normal PR CI and do
not rebuild plugin images there. On the existing ARM64 validation environment:

```sh
docker build -f Dockerfile.full --target plugin-build-80 -t ministack-spike-compiler80:local .
docker build -f Dockerfile.full --target plugin-build -t ministack-iam-stage7-build:local .
uv run --extra dev pytest contrib/mysql-iam-plugin/tests/test_connections.py -q
```

Requires Docker, OpenSSL, a native `mysql` client on PATH, and the repo's dev
dependencies. The Python helper image is `ghcr.io/ministackorg/ministack:full`;
local source is mounted read-only. Fixtures own and remove their containers,
volumes and network; Compose is unnecessary. The native client covers cold-cache
plaintext RSA authentication because the installed PyMySQL 1.2.3 cold RSA path
returns no packet to its caller. PyMySQL covers TLS and warmed password logins.

This is not yet a provisioned integration. The adapter only forwards QUERY,
QUIT, INIT_DB and PING; reauthentication, prepared statements and compression
are unsupported. Frames are capped at 1 MiB, socket operations at five seconds,
and idle sessions at ten seconds. These bounds are not a whole-handshake
deadline or a concurrency limit. Backend TLS is encrypted but its certificate
is not verified. Host-specific account selection is unproven: fixtures use `%`
accounts and MySQL sees the proxy's loopback address. Broader client compatibility
and fragmentation/multi-statement behavior still need validation.

Item 8 of #1744 must provide isolated backend networking, endpoint/resource and
capability provisioning, lifecycle handling, TLS certificates, and tests through
normal MiniStack provisioning before activating the accepting shim. It should
expose a supported internal decision API rather than the fixture's private seam.
The authorizer fixture creates local state; it is not production provisioning
or validation against AWS. Token expiry affects new logins, not existing sessions.

## Bundled compatibility artifact

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
