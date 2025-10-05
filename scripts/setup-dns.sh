#!/bin/bash
set -e

DOMAIN="sidemantic.com"

# Get Cloudflare credentials
if [ -z "$CLOUDFLARE_API_TOKEN" ]; then
  echo "Error: CLOUDFLARE_API_TOKEN environment variable not set"
  echo "Get your API token from: https://dash.cloudflare.com/profile/api-tokens"
  exit 1
fi

# Get zone ID
ZONE_ID=$(curl -s -X GET "https://api.cloudflare.com/client/v4/zones?name=${DOMAIN}" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  -H "Content-Type: application/json" | jq -r '.result[0].id')

if [ "$ZONE_ID" = "null" ]; then
  echo "Error: Could not find zone for ${DOMAIN}"
  exit 1
fi

echo "Found zone ID: $ZONE_ID"

# GitHub Pages IPs
IPS=(
  "185.199.108.153"
  "185.199.109.153"
  "185.199.110.153"
  "185.199.111.153"
)

# Add A records for apex domain
for IP in "${IPS[@]}"; do
  echo "Adding A record: @ -> $IP"
  curl -s -X POST "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records" \
    -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
    -H "Content-Type: application/json" \
    --data "{\"type\":\"A\",\"name\":\"@\",\"content\":\"${IP}\",\"ttl\":1,\"proxied\":false}" | jq -r '.success'
done

# Add CNAME for www
echo "Adding CNAME record: www -> sidequery.github.io"
curl -s -X POST "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  -H "Content-Type: application/json" \
  --data '{"type":"CNAME","name":"www","content":"sidequery.github.io","ttl":1,"proxied":false}' | jq -r '.success'

echo "✅ DNS records added successfully!"
echo "Note: Set Cloudflare SSL/TLS to 'Full' in the dashboard"
