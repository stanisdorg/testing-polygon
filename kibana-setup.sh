#!/usr/bin/env bash
# Initialize Kibana with index patterns and saved searches for FulfilBox
set -euo pipefail

KIBANA_URL="http://localhost:5601"

log_info() { echo -e "\033[0;32m[INFO]\033[0m  $*"; }
log_warn() { echo -e "\033[1;33m[WARN]\033[0m  $*"; }

wait_for_kibana() {
    log_info "Waiting for Kibana..."
    for i in $(seq 1 30); do
        if curl -s "${KIBANA_URL}/api/status" | grep -q "green\|available"; then
            log_info "Kibana is ready!"
            return 0
        fi
        sleep 2
    done
    log_warn "Kibana not ready after 60s"
    return 1
}

create_index_pattern() {
    local name="$1"
    local pattern="$2"
    local time_field="$3"

    log_info "Creating index pattern: $name ($pattern)"

    curl -s -X POST "${KIBANA_URL}/api/saved_objects/index-pattern" \
        -H "kbn-xsrf: true" \
        -H "Content-Type: application/json" \
        -d "{
            \"attributes\": {
                \"title\": \"${pattern}\",
                \"timeFieldName\": \"${time_field}\"
            }
        }" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  ID: {d.get(\"id\",\"?\")}')" || true
}

create_search() {
    local title="$1"
    local index_pattern_id="$2"
    local query="$3"

    log_info "Creating saved search: $title"

    curl -s -X POST "${KIBANA_URL}/api/saved_objects/search" \
        -H "kbn-xsrf: true" \
        -H "Content-Type: application/json" \
        -d "{
            \"attributes\": {
                \"title\": \"${title}\",
                \"description\": \"FulfilBox log search\",
                \"kibanaSavedObjectMeta\": {
                    \"searchSourceJSON\": \"{\\\"index\\\":\\\"${index_pattern_id}\\\",\\\"query\\\":{\\\"query\\\":\\\"${query}\\\"}}\"
                },
                \"columns\": [\"container.name\",\"log.level\",\"message\",\"@timestamp\"],
                \"sort\": [[\"@timestamp\",\"desc\"]]
            }
        }" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  ID: {d.get(\"id\",\"?\")}')" || true
}

main() {
    wait_for_kibana

    # Create index pattern for FulfilBox logs
    create_index_pattern "fulfilbox-logs" "fulfilbox-logs-*" "@timestamp"

    # Create index pattern for Docker logs
    create_index_pattern "docker-logs" "filebeat-*" "@timestamp"

    log_info "Kibana initialization complete!"
    log_info "Open: ${KIBANA_URL}"
    log_info "Discover → select 'fulfilbox-logs*' index pattern"
    log_info "Search for trace_id: kql query → trace_id:your-trace-id"
}

main "$@"
