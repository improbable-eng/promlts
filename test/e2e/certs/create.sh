#!/bin/zsh

openssl req \
  -new \
  -x509 \
  -nodes \
  -days 99999 \
  -subj '/CN=my-ca' \
  -keyout testca.key \
  -out testca.crt

openssl genrsa -out testserver.key 2048
openssl genrsa -out testclient.key 2048

openssl req \
  -new \
  -key testserver.key \
  -subj "/CN=e2e_receive_mtls_single_ingestor-receive-ingestor" \
  -out e2e_receive_mtls_single_ingestor_server.csr

openssl req \
  -new \
  -key testclient.key \
  -subj "/CN=e2e_receive_mtls_single_ingestor-prometheus-1" \
  -out e2e_receive_mtls_single_ingestor_client.csr

openssl x509 \
  -req \
  -in e2e_receive_mtls_single_ingestor_server.csr \
  -CA testca.crt \
  -CAkey testca.key \
  -CAcreateserial \
  -days 99999 \
  -extfile <(
    cat <<-EOF
basicConstraints = CA:FALSE
nsCertType = server
nsComment = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = e2e_receive_mtls_single_ingestor-receive-ingestor
EOF
  ) \
  -out e2e_receive_mtls_single_ingestor_server.crt

openssl x509 \
  -req \
  -in e2e_receive_mtls_single_ingestor_client.csr \
  -CA testca.crt \
  -CAkey testca.key \
  -CAcreateserial \
  -days 99999 \
  -extfile <(
    cat <<-EOF
basicConstraints = CA:FALSE
nsCertType = client
nsComment = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = e2e_receive_mtls_single_ingestor-prometheus-1
EOF
  ) \
  -out e2e_receive_mtls_single_ingestor_client.crt
