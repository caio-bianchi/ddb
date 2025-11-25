#!/usr/bin/env bash
set -euo pipefail

# ------------------------------
# Uso:
#   ./run_sharding_bench.sh get
#   ./run_sharding_bench.sh put
#   ./run_sharding_bench.sh both   # (opcional, default)
# ------------------------------

OP="${1:-both}"   # get | put | both

if [[ "$OP" != "get" && "$OP" != "put" && "$OP" != "both" ]]; then
  echo "Uso: $0 [get|put|both]"
  exit 1
fi

COORD_ADDR="127.0.0.1:9000"

# Lista de shards a testar
SHARDS_LIST=(1 2 4 8)

# Fator de replicação (nós por shard)
R=1

# Workload
OPS=5000
CONC_PUT=100
CONC_GET=150

RESULTS_DIR="results"
LOG_DIR="logs"

mkdir -p "$RESULTS_DIR" "$LOG_DIR"

cleanup_procs() {
  local pids=("$@")
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
    fi
  done
}

for S in "${SHARDS_LIST[@]}"; do
  echo
  echo "====================================================="
  echo "   SHARDS = $S, REPLICAS = $R, OP = $OP"
  echo "====================================================="

  # limpa dados antigos
  rm -rf raft-data storage_node_*.json || true

  # ---------- Coordinator ----------
  echo "[S=$S] Subindo coordinator..."
  go run ./cmd/coordinator \
    --address="${COORD_ADDR}" \
    --shards="${S}" \
    > "${LOG_DIR}/coord_S${S}.log" 2>&1 &
  COORD_PID=$!

  NODE_PIDS=()
  NODE_ID=1

  # ---------- Nodes ----------
  for shard in $(seq 0 $((S - 1))); do
    for rep in $(seq 0 $((R - 1))); do
      RPC_PORT=$((9100 + NODE_ID))
      RAFT_PORT=$((9200 + NODE_ID))

      go run ./cmd/node \
        --id="${NODE_ID}" \
        --rpc="127.0.0.1:${RPC_PORT}" \
        --raft="127.0.0.1:${RAFT_PORT}" \
        --shard="${shard}" \
        --coord="${COORD_ADDR}" \
        > "${LOG_DIR}/node_${NODE_ID}_S${S}.log" 2>&1 &

      NODE_PIDS+=($!)
      NODE_ID=$((NODE_ID + 1))
    done
  done

  echo "[S=$S] Aguardando cluster estabilizar..."
  sleep 6

  # ---------- GET ----------
  if [[ "$OP" == "get" || "$OP" == "both" ]]; then
    echo "[S=$S] Rodando benchmark GET..."
    OUT_GET="${RESULTS_DIR}/get_S${S}_R${R}.csv"

    go run ./cmd/metrics \
      --coord="${COORD_ADDR}" \
      --op="get" \
      --count="${OPS}" \
      --conc="${CONC_GET}" \
      --csv="${OUT_GET}"
  fi

  # ---------- PUT ----------
  if [[ "$OP" == "put" || "$OP" == "both" ]]; then
    echo "[S=$S] Rodando benchmark PUT..."
    OUT_PUT="${RESULTS_DIR}/put_S${S}_R${R}.csv"

    go run ./cmd/metrics \
      --coord="${COORD_ADDR}" \
      --op="put" \
      --count="${OPS}" \
      --conc="${CONC_PUT}" \
      --csv="${OUT_PUT}"
  fi

  # ---------- Shutdown ----------
  cleanup_procs "$COORD_PID" "${NODE_PIDS[@]}"
  sleep 1
  echo "[S=$S] Concluído."
done

echo "====================================================="
echo " Experimentos finalizados!"
echo " Resultados em: results/shards_S*_R${R}_*.csv"
echo "====================================================="
