/*
 * MiniStack's L0 emulation of Aurora MySQL's AWSAuthenticationPlugin.
 *
 * This plugin intentionally rejects every authentication attempt. It exists
 * so local Aurora users can be created with the same authentication-plugin
 * name as AWS and retain that name in mysql.user. Token authentication is a
 * later fidelity level; accepting credentials here would be unsafe.
 */

#include <stddef.h>

// MYSQL_ABI_CHECK omits MySQL's internal compiler headers. Dynamic plugin
// declarations still need the public-symbol visibility wrapper they provide.
#ifndef MY_ATTRIBUTE
#define MY_ATTRIBUTE(attributes) __attribute__(attributes)
#endif

#include <mysql/plugin_auth.h>

static int reject_authentication(MYSQL_PLUGIN_VIO *vio,
                                 MYSQL_SERVER_AUTH_INFO *info) {
  (void)vio;
  (void)info;
  return CR_ERROR;
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
    NULL,
    reject_authentication,
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
  "Aurora IAM authentication compatibility plugin (reject-all L0)",
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
