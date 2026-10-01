#!/usr/bin/env bash
# Generates a throwaway CA and a server certificate for localhost, used by the TLS listener.
set -euo pipefail

dir="$(cd "$(dirname "$0")/.." && pwd)/certs"

if [ -f "$dir/server.pem" ] && [ -f "$dir/ca.pem" ]; then
  exit 0
fi

mkdir -p "$dir"
ext="$(mktemp)"
trap 'rm -f "$ext" "$dir/server.csr" "$dir/ca.srl"' EXIT
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n' > "$ext"

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj '/CN=amqplib-soak CA' \
  -keyout "$dir/ca-key.pem" -out "$dir/ca.pem" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj '/CN=localhost' \
  -keyout "$dir/server-key.pem" -out "$dir/server.csr" 2>/dev/null
openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca-key.pem" -CAcreateserial \
  -days 3650 -extfile "$ext" -out "$dir/server.pem" 2>/dev/null

# the broker runs as its own user inside the container and must be able to read the key
chmod 644 "$dir"/*.pem

echo "wrote certificates to $dir"
