# Running Thanos with HTTPS and basic authentication

Thanos supports basic authentication and TLS. This is **experimental** and might change in the future.

To specify which HTTP TLS configuration file to load, use the `--http.config` flag. The file is written in [YAML format](https://en.wikipedia.org/wiki/YAML), defined by the scheme described below.

## Scheme

Brackets indicate that a parameter is optional. For non-list parameters the value is set to the specified default. The file is read upon every http request, such as any change in the configuration and the certificates is picked up immediately.

Generic placeholders are defined as follows:

- `<boolean>`: a boolean that can take the values `true` or `false`
- `<filename>`: a valid path in the current working directory
- `<secret>`: a regular string that is a secret, such as a password
- `<string>`: a regular string

```yaml
tls_server_config:
  # Certificate and key files for server to use to authenticate to client.
  cert_file: <filename>
  key_file: <filename>

  # Server policy for client authentication. Maps to ClientAuth Policies.
  # For more detail on clientAuth options:
  # https://golang.org/pkg/crypto/tls/#ClientAuthType
  [ client_auth_type: <string> | default = "NoClientCert" ]

  # CA certificate for client certificate authentication to the server.
  [ client_ca_file: <filename> ]

  # Minimum TLS version that is acceptable.
  [ min_version: <string> | default = "TLS12" ]

  # Maximum TLS version that is acceptable.
  [ max_version: <string> | default = "TLS13" ]

  # List of supported cipher suites for TLS versions up to TLS 1.2. If empty,
  # Go default cipher suites are used. Available cipher suites are documented
  # in the go documentation:
  # https://golang.org/pkg/crypto/tls/#pkg-constants
  [ cipher_suites:
    [ - <string> ] ]

  # prefer_server_cipher_suites controls whether the server selects the
  # client's most preferred ciphersuite, or the server's most preferred
  # ciphersuite. If true then the server's preference, as expressed in
  # the order of elements in cipher_suites, is used.
  [ prefer_server_cipher_suites: <bool> | default = true ]

  # Elliptic curves that will be used in an ECDHE handshake, in preference
  # order. Available curves are documented in the go documentation:
  # https://golang.org/pkg/crypto/tls/#CurveID
  [ curve_preferences:
    [ - <string> ] ]

http_server_config:
  # Enable HTTP/2 support. Note that HTTP/2 is only supported with TLS.
  # This can not be changed on the fly.
  [ http2: <boolean> | default = true ]

# Usernames and hashed passwords that have full access to the web
# server via basic authentication. If empty, no basic authentication is
# required. Passwords are hashed with bcrypt.
basic_auth_users:
  [ <string>: <secret> ... ]
```

## Example

An example configuration file is provided below,

```yaml
# A certificate and a key file are needed.
tls_server_config:
  cert_file: server.crt
  key_file: server.key

# Usernames and passwords required to connect to Thanos.
# Passwords are hashed with bcrypt.
basic_auth_users:
  alice: $2y$10$mDwo.lAisC94iLAyP81MCesa29IzH37oigHC/42V2pdJlUprsJPze
  bob: $2y$10$hLqFl9jSjoAAy95Z/zw8Ye8wkdMBM8c5Bn1ptYqP/AXyV0.oy0S8m
```
