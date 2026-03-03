#!/bin/bash
#SBATCH --job-name=abasto_03_baseline
#SBATCH --output=outputs/slurm_%j_03_baseline.out
#SBATCH --error=outputs/slurm_%j_03_baseline.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=48G
#SBATCH --time=02:00:00

set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== [03_baseline] Inicio: $(date) ==="
echo "Nodo: $SLURMD_NODENAME"

export POLARS_MAX_THREADS=$SLURM_CPUS_PER_TASK

uv run python scripts/03_baseline.py

echo "=== [03_baseline] Fin: $(date) ==="
