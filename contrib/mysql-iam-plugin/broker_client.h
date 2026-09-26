// Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
// Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
#ifndef MINISTACK_IAM_BROKER_CLIENT_H
#define MINISTACK_IAM_BROKER_CLIENT_H

#include <arpa/inet.h>
#include <curl/curl.h>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

namespace ministack_iam {

// Only trusted container provisioning sets these. No URL, account, endpoint,
// or resource identifier is accepted from a database client. Numeric IPv4
// avoids unbounded synchronous DNS resolution in libcurl builds without c-ares.
inline bool configuration(std::string &url, std::string &capability) {
  const char *host = std::getenv("MINISTACK_RDS_IAM_BROKER_HOST");
  const char *port = std::getenv("MINISTACK_RDS_IAM_BROKER_PORT");
  const char *cap = std::getenv("MINISTACK_RDS_IAM_CAPABILITY");
  in_addr address{};
  if (!host || inet_pton(AF_INET, host, &address) != 1 || !port || !*port ||
      std::strlen(port) > 5 || !cap || std::strlen(cap) != 64)
    return false;
  unsigned number = 0;
  for (const char *p = port; *p; ++p) {
    if (*p < '0' || *p > '9') return false;
    number = number * 10 + (*p - '0');
  }
  if (number == 0 || number > 65535) return false;
  for (const char *p = cap; *p; ++p)
    if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')))
      return false;
  url = std::string("http://") + host + ":" + port + "/_ministack/rds/iam-auth";
  capability = cap;
  return true;
}

inline std::string json_string(const char *data, size_t length) {
  std::string result = "\"";
  const char hex[] = "0123456789abcdef";
  for (size_t i = 0; i < length; ++i) {
    unsigned char c = static_cast<unsigned char>(data[i]);
    if (c == '"' || c == '\\') {
      result += '\\';
      result += c;
    } else if (c < 0x20) {
      result += "\\u00";
      result += hex[c >> 4];
      result += hex[c & 15];
    } else {
      result += c;
    }
  }
  return result + '"';
}

inline size_t receive_response(char *data, size_t size, size_t count, void *out) {
  auto &response = *static_cast<std::string *>(out);
  // libcurl supplies size=1. Check multiplication anyway, then enforce the cap.
  if (size != 0 && count > 1024 / size) return 0;
  const size_t bytes = size * count;
  if (bytes > 1024 - response.size()) return 0;
  try {
    response.append(data, bytes);
  } catch (...) {
    return 0;
  }
  return bytes;
}

inline bool authorize(const char *user, size_t user_length,
                      const unsigned char *packet, int packet_length) {
  if (!user || user_length == 0 || user_length > 256 ||
      std::memchr(user, 0, user_length) || !packet || packet_length < 1 ||
      packet_length > 65537 || packet[packet_length - 1] != 0 ||
      std::memchr(packet, 0, packet_length - 1))
    return false;
  std::string url, capability;
  if (!configuration(url, capability)) return false;
  const std::string body = "{\"username\":" + json_string(user, user_length) +
      ",\"token\":" + json_string(reinterpret_cast<const char *>(packet), packet_length - 1) + "}";
  if (body.size() > 70 * 1024) return false;
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), curl_easy_cleanup);
  if (!curl) return false;
  curl_slist *raw_headers = nullptr;
  for (const auto &header : {std::string("Content-Type: application/json"),
                            std::string("Expect:"),
                            "X-Ministack-RDS-Capability: " + capability}) {
    curl_slist *next = curl_slist_append(raw_headers, header.c_str());
    if (!next) {
      curl_slist_free_all(raw_headers);
      return false;
    }
    raw_headers = next;
  }
  std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw_headers, curl_slist_free_all);
  std::string response;
  // Never follow redirects or honor proxy environment variables: either could
  // disclose the token and resource capability outside the configured broker.
  if (curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str()) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_PROXY, "") != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 0L) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, 1000L) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, 3000L) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get()) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.c_str()) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size())) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, receive_response) != CURLE_OK ||
      curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response) != CURLE_OK)
    return false;
  long status = 0;
  if (curl_easy_perform(curl.get()) != CURLE_OK ||
      curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status) != CURLE_OK)
    return false;
  // This is an internal versioned-by-source contract, not general JSON input.
  // Fail closed on duplicate fields, trailing data, coercions, and new shapes.
  return status == 200 && response == "{\"allowed\":true}";
}

}  // namespace ministack_iam
#endif
