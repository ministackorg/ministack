/*
 * MiniStack's Aurora MySQL AWSAuthenticationPlugin.
 *
 * Like AWS's plugin it asks the client for mysql_clear_password and reads the
 * RDS IAM token as the password. MiniStack decides: the plugin forwards the
 * user and token to its broker and accepts only an explicit allow. A missing
 * config, a timeout or any malformed answer denies.
 */

#include <netdb.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>

// MYSQL_ABI_CHECK omits MySQL's internal compiler headers. Dynamic plugin
// declarations still need the public-symbol visibility wrapper they provide.
#ifndef MY_ATTRIBUTE
#define MY_ATTRIBUTE(attributes) __attribute__(attributes)
#endif

#include <mysql/plugin_auth.h>

// Written by MiniStack when the container is ready: "<host> <port> <capability>".
static const char CONFIG_PATH[] = "/etc/ministack/rds-iam.conf";

static bool json_string(std::string &out, const std::string &value) {
  out += '"';
  for (unsigned char c : value) {
    if (c < 0x20 || c == 0x7f) return false;
    if (c == '"' || c == '\\') out += '\\';
    out += static_cast<char>(c);
  }
  out += '"';
  return true;
}

static bool broker_allows(const std::string &user, const std::string &token) {
  char host[256], port[6], capability[65];
  FILE *config = fopen(CONFIG_PATH, "r");
  if (!config) return false;
  int fields = fscanf(config, "%255s %5s %64s", host, port, capability);
  fclose(config);
  if (fields != 3 || strlen(capability) != 64) return false;

  std::string body = "{\"username\":";
  if (!json_string(body, user)) return false;
  body += ",\"token\":";
  if (!json_string(body, token)) return false;
  body += '}';
  std::string request = "POST /_ministack/rds/iam-auth HTTP/1.1\r\nHost: ";
  request += host;
  request += "\r\nContent-Type: application/json\r\nX-Ministack-RDS-Capability: ";
  request += capability;
  request += "\r\nContent-Length: " + std::to_string(body.size());
  request += "\r\nConnection: close\r\n\r\n" + body;

  struct addrinfo hints = {}, *addresses = nullptr;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(host, port, &hints, &addresses) != 0) return false;
  int fd = -1;
  struct timeval timeout = {3, 0};
  for (struct addrinfo *a = addresses; a && fd < 0; a = a->ai_next) {
    fd = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
    if (fd < 0) continue;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout);
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    if (connect(fd, a->ai_addr, a->ai_addrlen) != 0) {
      close(fd);
      fd = -1;
    }
  }
  freeaddrinfo(addresses);
  if (fd < 0) return false;

  bool sent = true;
  for (size_t offset = 0; sent && offset < request.size();) {
    ssize_t n = send(fd, request.data() + offset, request.size() - offset, MSG_NOSIGNAL);
    sent = n > 0;
    if (sent) offset += n;
  }
  std::string response;
  char buffer[1024];
  ssize_t n;
  while (sent && response.size() < 8192 && (n = recv(fd, buffer, sizeof buffer, 0)) > 0)
    response.append(buffer, n);
  close(fd);
  return sent && response.compare(0, 13, "HTTP/1.1 200 ") == 0 &&
         response.find("{\"allowed\":true}") != std::string::npos;
}

static int authenticate(MYSQL_PLUGIN_VIO *vio, MYSQL_SERVER_AUTH_INFO *info) {
  unsigned char *packet = nullptr;
  int size = vio->read_packet(vio, &packet);
  if (size < 0 || !packet) return CR_ERROR;
  info->password_used = PASSWORD_USED_YES;
  std::string token(reinterpret_cast<char *>(packet), size);
  if (!token.empty() && token.back() == '\0') token.pop_back();
  std::string user(info->user_name, info->user_name_length);
  // Anonymous accounts would authorize a name other than the account's.
  if (token.empty() || user.empty() || user != info->authenticated_as)
    return CR_ERROR;
  return broker_allows(user, token) ? CR_OK : CR_ERROR;
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
