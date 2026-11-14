#!/bin/sh
# Creates the "atomic" catalog (FILE storage under /warehouse) and grants content management.
#   realm POLARIS   always; token from Polaris's built-in OAuth endpoint
#   realm EXTERNAL  only when KEYCLOAK_TOKEN_URL is set (docker-compose.keycloak.yml); token from Keycloak
set -e
apk add --no-cache jq >/dev/null

API=http://polaris:8181

setup_realm() {
  realm=$1
  token=$2
  code=$(curl -s -o /tmp/out -w '%{http_code}' -H "Authorization: Bearer $token" -H "Polaris-Realm: $realm" \
    "$API/api/management/v1/catalogs/atomic")
  if [ "$code" = "200" ]; then
    echo "[$realm] catalog 'atomic' already exists"
  else
    curl --fail-with-body -s -H "Authorization: Bearer $token" -H "Polaris-Realm: $realm" \
      -H 'Content-Type: application/json' "$API/api/management/v1/catalogs" -d '{
        "catalog": {
          "name": "atomic",
          "type": "INTERNAL",
          "readOnly": false,
          "properties": {"default-base-location": "file:///warehouse/atomic"},
          "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///warehouse/atomic"]}
        }
      }'
    echo "[$realm] created catalog 'atomic'"
  fi
  curl --fail-with-body -s -X PUT -H "Authorization: Bearer $token" -H "Polaris-Realm: $realm" \
    -H 'Content-Type: application/json' \
    "$API/api/management/v1/catalogs/atomic/catalog-roles/catalog_admin/grants" \
    -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'
}

POLARIS_TOKEN=$(curl --fail-with-body -s "$API/api/catalog/v1/oauth/tokens" -H "Polaris-Realm: POLARIS" \
  --user root:s3cr3t -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
setup_realm POLARIS "$POLARIS_TOKEN"

if [ -n "$KEYCLOAK_TOKEN_URL" ]; then
  KC_TOKEN=$(curl --fail-with-body -s "$KEYCLOAK_TOKEN_URL" \
    --user client1:s3cr3t -d grant_type=client_credentials | jq -r .access_token)
  setup_realm EXTERNAL "$KC_TOKEN"
fi

echo "Polaris setup done"
touch /tmp/polaris-setup-done
tail -f /dev/null
