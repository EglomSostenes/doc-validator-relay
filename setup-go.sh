#!/bin/bash
# ==============================================================================
# setup-go.sh — Doc-Validator · Go Relay
# Sobe: relay
# Pré-requisito: Postgres e RabbitMQ já healthy.
#
# Pode ser chamado diretamente ou pelo setup.sh principal.
# ==============================================================================
set -euo pipefail

# ──────────────────────────────────────────────
# GUARD
# ──────────────────────────────────────────────
[[ "${GO_BOOTSTRAP_LOADED:-}" == "1" ]] && return 0 || true

# ──────────────────────────────────────────────
# LOCALIZAÇÃO
# ──────────────────────────────────────────────
GO_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GO_COMPOSE_FILE="$GO_SCRIPT_DIR/go-relay/docker-compose-go-infra.yml"
GO_DIR="$GO_SCRIPT_DIR/go-relay"

# ──────────────────────────────────────────────
# IMPORTA LIB COMPARTILHADA
# ──────────────────────────────────────────────
# shellcheck source=./lib/common.sh
source "$GO_SCRIPT_DIR/lib/common.sh"

# ──────────────────────────────────────────────
# MAIN DO GO RELAY
# ──────────────────────────────────────────────
setup_go() {
  local standalone=${1:-0}

  if [[ "$standalone" == "1" ]]; then
    _init_log
    log_banner "Go Relay" "relay"
    check_dependencies
    ensure_env "$GO_DIR" "go-relay"
    ensure_network

    log_warn "Modo standalone: assumindo que Postgres e RabbitMQ já estão no ar."
    log_warn "Se não estiverem, execute setup-infra.sh primeiro."
  fi

  ensure_env "$GO_DIR" "go-relay"

  log_section "Go Relay"

  log_step "Subindo containers Go"
  docker compose -f "$GO_COMPOSE_FILE" up -d 2>&1 | tee -a "$LOG_FILE"

  log_step "Aguardando relay"
  wait_for_running "relay" "$GO_COMPOSE_FILE"

  log_ok "Go Relay pronto!"
  export GO_READY=1
}

# ──────────────────────────────────────────────
# EXECUÇÃO DIRETA
# ──────────────────────────────────────────────
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  GO_BOOTSTRAP_LOADED=1
  setup_go 1
  echo ""
  log_ok "━━━  Go Relay no ar  ━━━"
  echo -e "  Logs: docker compose -f go-relay/docker-compose-go-infra.yml logs -f relay"
fi

GO_BOOTSTRAP_LOADED=1
