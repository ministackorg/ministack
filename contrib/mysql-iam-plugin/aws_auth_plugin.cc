/*
 * MiniStack's Aurora MySQL AWSAuthenticationPlugin compatibility adapter.
 *
 * Normal builds reject logins until isolated proxy provisioning is available.
 * Only isolated connection tests currently enable the proxy-approved path.
 * That path does not verify credentials: clients must not reach it directly.
 */

#include <stddef.h>
#include <string.h>

#ifndef MINISTACK_IAM_PROXY_AUTH
#define MINISTACK_IAM_PROXY_AUTH 0
#endif

// MYSQL_ABI_CHECK omits MySQL's internal compiler headers. Dynamic plugin
// declarations still need the public-symbol visibility wrapper they provide.
#ifndef MY_ATTRIBUTE
#define MY_ATTRIBUTE(attributes) __attribute__(attributes)
#endif

#include <mysql/plugin_auth.h>

static int authenticate(MYSQL_PLUGIN_VIO *vio,
                                 MYSQL_SERVER_AUTH_INFO *info) {
#if MINISTACK_IAM_PROXY_AUTH
  if (!vio || !vio->read_packet || !info) return CR_ERROR;
  unsigned char *packet = nullptr;
  info->password_used = PASSWORD_USED_YES;
  int size = vio->read_packet(vio, &packet);
  if (size < 1 || !packet || packet[size - 1] != 0 || !info->user_name ||
      info->user_name_length == 0 ||
      strlen(info->authenticated_as) != info->user_name_length ||
      memcmp(info->authenticated_as, info->user_name, info->user_name_length))
    return CR_ERROR;
  return CR_OK;  // Python proxy owns IAM admission; isolation is mandatory.
#else
  (void)vio;
  (void)info;
  return CR_ERROR;
#endif
}

static int generate_authentication_string(char *outbuf,
                                          unsigned int *outbuflen,
                                          const char *inbuf,
                                          unsigned int inbuflen) {
  (void)outbuf;
  (void)inbuf;
  (void)inbuflen;
  *outbuflen = 0;
  return 0;
}

static int validate_authentication_string(char *const inbuf,
                                          unsigned int buflen) {
  (void)inbuf;
  (void)buflen;
  return 0;
}

static int set_salt(const char *password, unsigned int password_len,
                    unsigned char *salt, unsigned char *salt_len) {
  (void)password;
  (void)password_len;
  (void)salt;
  *salt_len = 0;
  return 0;
}

static struct st_mysql_auth aws_auth_handler = {
    MYSQL_AUTHENTICATION_INTERFACE_VERSION,
#if MINISTACK_IAM_PROXY_AUTH
    "ministack_iam_gate_v1",
#else
    NULL,
#endif
    authenticate,
    generate_authentication_string,
    validate_authentication_string,
    set_salt,
    AUTH_FLAG_PRIVILEGED_USER_FOR_PASSWORD_CHANGE,
    NULL,
};

mysql_declare_plugin(aws_auth_plugin) {
  MYSQL_AUTHENTICATION_PLUGIN,
  &aws_auth_handler,
  "AWSAuthenticationPlugin",
  "MiniStack",
  "Aurora IAM authentication compatibility plugin",
  PLUGIN_LICENSE_GPL,
  NULL,
  NULL,
  NULL,
  0x0100,
  NULL,
  NULL,
  NULL,
  0,
} mysql_declare_plugin_end;
