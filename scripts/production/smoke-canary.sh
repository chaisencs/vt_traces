#!/usr/bin/env bash
set -euo pipefail

WRITE_BASE_URL=${VT_CANARY_WRITE_URL:-}
READ_BASE_URL=${VT_CANARY_READ_URL:-}
API_BEARER_TOKEN=${VT_CANARY_API_BEARER_TOKEN:-}
TIMEOUT_SECS=${VT_CANARY_TIMEOUT_SECS:-15}
INSERT_HEALTH_URL=${VT_CANARY_INSERT_HEALTH_URL:-}
SELECT_HEALTH_URL=${VT_CANARY_SELECT_HEALTH_URL:-}
METRICS_URL=${VT_CANARY_METRICS_URL:-}

if [[ -z "$WRITE_BASE_URL" || -z "$READ_BASE_URL" ]]; then
  echo "VT_CANARY_WRITE_URL and VT_CANARY_READ_URL are required" >&2
  exit 64
fi

WRITE_BASE_URL=${WRITE_BASE_URL%/}
READ_BASE_URL=${READ_BASE_URL%/}
INSERT_HEALTH_URL=${INSERT_HEALTH_URL:-$WRITE_BASE_URL/healthz}
SELECT_HEALTH_URL=${SELECT_HEALTH_URL:-$READ_BASE_URL/healthz}
METRICS_URL=${METRICS_URL:-$READ_BASE_URL/metrics}

TRACE_ID="trace-canary-$(date +%s)-$$"
SERVICE_NAME="canary-throughput"
ROUTE_VALUE="/canary/$TRACE_ID"
NOW_SECS=$(date +%s)
START_NS=$(((NOW_SECS - 60) * 1000000000))
END_NS=$(((NOW_SECS + 60) * 1000000000))

curl_api() {
  if [[ -n "$API_BEARER_TOKEN" ]]; then
    curl -fsS -H "authorization: Bearer $API_BEARER_TOKEN" "$@"
  else
    curl -fsS "$@"
  fi
}

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

payload_file=$TMPDIR/payload.json
cat >"$payload_file" <<EOF
{
  "resource_spans": [
    {
      "resource_attributes": [
        { "key": "service.name", "value": { "kind": "string", "value": "$SERVICE_NAME" } }
      ],
      "scope_spans": [
        {
          "scope_name": "prod.canary",
          "scope_version": "1.0.0",
          "scope_attributes": [],
          "spans": [
            {
              "trace_id": "$TRACE_ID",
              "span_id": "span-$TRACE_ID",
              "parent_span_id": null,
              "name": "throughput_canary",
              "start_time_unix_nano": $START_NS,
              "end_time_unix_nano": $END_NS,
              "attributes": [
                { "key": "http.route", "value": { "kind": "string", "value": "$ROUTE_VALUE" } },
                { "key": "deployment.tier", "value": { "kind": "string", "value": "throughput-canary" } }
              ],
              "status": { "code": 0, "message": "OK" }
            }
          ]
        }
      ]
    }
  ]
}
EOF

curl -fsS "$INSERT_HEALTH_URL" >/dev/null
curl -fsS "$SELECT_HEALTH_URL" >/dev/null

curl_api \
  -H "content-type: application/json" \
  -X POST \
  --data-binary "@$payload_file" \
  "$WRITE_BASE_URL/v1/traces" >/dev/null

deadline=$((SECONDS + TIMEOUT_SECS))

wait_for_body() {
  local url=$1
  local needle=$2
  local outfile=$3
  while (( SECONDS < deadline )); do
    if curl_api "$url" >"$outfile" 2>/dev/null; then
      if grep -q "$needle" "$outfile"; then
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

TRACE_URL="$READ_BASE_URL/api/traces/$TRACE_ID"
SEARCH_URL="$READ_BASE_URL/api/v1/traces/search?start_unix_nano=$START_NS&end_unix_nano=$END_NS&service_name=$SERVICE_NAME&limit=10"
SERVICES_URL="$READ_BASE_URL/api/v1/services"
FIELD_VALUES_URL="$READ_BASE_URL/api/search/tag/http.route/values?limit=50"

wait_for_body "$TRACE_URL" "$TRACE_ID" "$TMPDIR/trace.json" || {
  echo "trace-by-id probe failed" >&2
  cat "$TMPDIR/trace.json" >&2 || true
  exit 1
}

wait_for_body "$SEARCH_URL" "$TRACE_ID" "$TMPDIR/search.json" || {
  echo "search probe failed" >&2
  cat "$TMPDIR/search.json" >&2 || true
  exit 1
}

wait_for_body "$SERVICES_URL" "$SERVICE_NAME" "$TMPDIR/services.json" || {
  echo "services probe failed" >&2
  cat "$TMPDIR/services.json" >&2 || true
  exit 1
}

wait_for_body "$FIELD_VALUES_URL" "$ROUTE_VALUE" "$TMPDIR/field-values.json" || {
  echo "field-values probe failed" >&2
  cat "$TMPDIR/field-values.json" >&2 || true
  exit 1
}

metrics_file=$TMPDIR/metrics.txt
curl -fsS "$METRICS_URL" >"$metrics_file"
grep -q '^vt_storage_trace_live_update_queue_depth ' "$metrics_file"
grep -q '^vt_storage_trace_seal_queue_depth ' "$metrics_file"

echo "canary smoke passed"
echo "trace_id=$TRACE_ID"
echo "service_name=$SERVICE_NAME"
echo "route_value=$ROUTE_VALUE"
echo "trace_url=$TRACE_URL"
echo "search_url=$SEARCH_URL"
