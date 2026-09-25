// UNBUNDLED: unsafe unless MySQL is inaccessible except through the gatekeeper.
#include <stddef.h>
#include <string.h>
#define MY_ATTRIBUTE(attributes) __attribute__(attributes)
#include <mysql/plugin_auth.h>

static int accept(MYSQL_PLUGIN_VIO *vio, MYSQL_SERVER_AUTH_INFO *info) {
  unsigned char *packet = nullptr;
  info->password_used = PASSWORD_USED_YES;
  int size = vio->read_packet(vio, &packet);
  if (size < 1 || packet[size - 1] != 0 || !info->user_name ||
      strlen(info->authenticated_as) != info->user_name_length ||
      memcmp(info->authenticated_as, info->user_name, info->user_name_length))
    return CR_ERROR;
  return CR_OK;  // No token, password, IAM, network, or TLS validation here.
}
static int generate(char *, unsigned int *len, const char *, unsigned int) {
  *len = 0;
  return 0;
}
static int validate(char *const, unsigned int) { return 0; }
static int salt(const char *, unsigned int, unsigned char *, unsigned char *len) {
  *len = 0;
  return 0;
}
static struct st_mysql_auth handler = {
  MYSQL_AUTHENTICATION_INTERFACE_VERSION, "ministack_iam_gate_v1", accept,
  generate, validate, salt, AUTH_FLAG_PRIVILEGED_USER_FOR_PASSWORD_CHANGE, NULL,
};
mysql_declare_plugin(aws_auth_plugin) {
  MYSQL_AUTHENTICATION_PLUGIN, &handler, "AWSAuthenticationPlugin", "MiniStack",
  "UNSAFE accepting shim for isolated proxy spike", PLUGIN_LICENSE_GPL,
  NULL, NULL, NULL, 0x0100, NULL, NULL, NULL, 0,
} mysql_declare_plugin_end;
