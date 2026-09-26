/*
 * MiniStack's AWSAuthenticationPlugin adapter (stage 7 of #1744).
 * No runtime capability provisioning until stage 8; absent config denies.
 */

#include <stddef.h>

// MYSQL_ABI_CHECK omits MySQL's internal compiler headers. Dynamic plugin
// declarations still need the public-symbol visibility wrapper they provide.
#ifndef MY_ATTRIBUTE
#define MY_ATTRIBUTE(attributes) __attribute__(attributes)
#endif

#include <mysql/plugin_auth.h>
#include "broker_client.h"

static int broker_authentication(MYSQL_PLUGIN_VIO *vio,
                                 MYSQL_SERVER_AUTH_INFO *info) {
  try {
    if (!vio || !info || !vio->read_packet) return CR_ERROR;
    info->password_used = PASSWORD_USED_YES;
    unsigned char *packet = nullptr;
    const int length = vio->read_packet(vio, &packet);
    // read_packet may populate user_name during the initial handshake. Never
    // authorize an anonymous/proxy account under a client-supplied identity.
    if (!info->user_name || info->user_name_length == 0 ||
        info->user_name_length > MYSQL_USERNAME_LENGTH ||
        std::strlen(info->authenticated_as) != info->user_name_length ||
        std::memcmp(info->authenticated_as, info->user_name, info->user_name_length))
      return CR_ERROR;
    if (ministack_iam::authorize(info->user_name, info->user_name_length, packet, length))
      return CR_OK;
  } catch (...) {
    // Never leak tokens/capabilities through errors or across the C plugin ABI.
  }
  return CR_ERROR;
}

static int initialize_broker(void *) {
  return curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK ? 0 : 1;
}

static int deinitialize_broker(void *) {
  curl_global_cleanup();
  return 0;
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
    "mysql_clear_password",
    broker_authentication,
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
  "RDS IAM authentication broker adapter",
  PLUGIN_LICENSE_GPL,
  initialize_broker,
  NULL,
  deinitialize_broker,
  0x0100,
  NULL,
  NULL,
  NULL,
  0,
} mysql_declare_plugin_end;
