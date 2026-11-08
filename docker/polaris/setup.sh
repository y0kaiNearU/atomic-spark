#!/bin/sh
# Creates the "atomic" catalog (FILE storage under /warehouse) and grants content management.
set -e
apk add --no-cache jq >/dev/null

API=http://polaris:8181
TOKEN=$(curl --fail-with-body -s "$API/api/catalog/v1/oauth/tokens" \
  --user root:s3cr3t -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

code=$(curl -s -o /tmp/out -w '%{http_code}' -H "Authorization: Bearer $TOKEN" \
  "$API/api/management/v1/catalogs/atomic")
if [ "$code" = "200" ]; then
  echo "Catalog 'atomic' already exists"
else
  curl --fail-with-body -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    "$API/api/management/v1/catalogs" -d '{
      "catalog": {
        "name": "atomic",
        "type": "INTERNAL",
        "readOnly": false,
        "properties": {"default-base-location": "file:///warehouse/atomic"},
        "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///warehouse/atomic"]}
      }
    }'
  echo "Created catalog 'atomic'"
fi

curl --fail-with-body -s -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  "$API/api/management/v1/catalogs/atomic/catalog-roles/catalog_admin/grants" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'

echo "Polaris setup done"
touch /tmp/polaris-setup-done
tail -f /dev/null
