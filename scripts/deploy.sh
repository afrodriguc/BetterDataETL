#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-better-wase-data-2}"
REGION="${REGION:-us-west1}"
RUNTIME="python312"

SHARED_DIR="$(cd "$(dirname "$0")/../shared" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*"; exit 1; }

declare -A ENTRY_POINTS=(
  [bronze_to_silver]="bronze_to_silver"
  [cleanup_on_delete]="cleanup_on_delete"
  [manual_etl_loader]="manual_etl_loader"
  [schema_alert]="schema_alert_pubsub"
  [schema_detector]="detect_schema"
  [status_alert]="status_alert_pubsub"
)

declare -A TRIGGERS=(
  [bronze_to_silver]="--trigger-event-filters=type=google.cloud.storage.object.v1.finalized,bucket=${PROJECT_ID}"
  [cleanup_on_delete]="--trigger-event-filters=type=google.cloud.storage.object.v1.deleted,bucket=${PROJECT_ID}"
  [manual_etl_loader]="--trigger-http --allow-unauthenticated"
  [schema_alert]="--trigger-topic=schema-changes"
  [schema_detector]="--trigger-event-filters=type=google.cloud.storage.object.v1.finalized,bucket=${PROJECT_ID}"
  [status_alert]="--trigger-topic=status-changes"
)

deploy_function() {
  local name="$1"
  local func_dir="$(cd "$(dirname "$0")/../functions/${name}" && pwd)"

  [[ ! -d "$func_dir" ]] && err "No se encontró el directorio: $func_dir"

  local entry="${ENTRY_POINTS[$name]}"
  local trigger="${TRIGGERS[$name]}"

  log "Desplegando: $name → entry: $entry"

  cp -r "$SHARED_DIR" "$func_dir/_shared"

  cp "$func_dir/requirements.txt" "$func_dir/requirements.txt.bak"
  sed 's|etl-carriers @ ../../shared|etl-carriers @ file:_shared|g' \
    "$func_dir/requirements.txt.bak" > "$func_dir/requirements.txt"

  gcloud functions deploy "$name" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --runtime="$RUNTIME" \
    --source="$func_dir" \
    --entry-point="$entry" \
    --set-env-vars="GCP_PROJECT=$PROJECT_ID,GCS_BUCKET=$PROJECT_ID" \
    --memory="1024MB" \
    --timeout="540s" \
    --gen2 \
    $trigger

  mv "$func_dir/requirements.txt.bak" "$func_dir/requirements.txt"
  rm -rf "$func_dir/_shared"

  log "✅ $name desplegada correctamente"
}

FUNCTIONS_TO_DEPLOY=(${@:-bronze_to_silver cleanup_on_delete manual_etl_loader schema_alert schema_detector status_alert})

log "Proyecto:  $PROJECT_ID"
log "Región:    $REGION"
log "Funciones: ${FUNCTIONS_TO_DEPLOY[*]}"
echo ""

for func in "${FUNCTIONS_TO_DEPLOY[@]}"; do
  deploy_function "$func"
done

log ""
log "🎉 Deploy completado"
