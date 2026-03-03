#!/bin/bash
#SBATCH --job-name=abasto_02_calidad
#SBATCH --output=outputs/slurm_%j_02_calidad.out
#SBATCH --error=outputs/slurm_%j_02_calidad.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=01:00:00

set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== [02_calidad] Inicio: $(date) ==="
echo "Nodo: $SLURMD_NODENAME"

export POLARS_MAX_THREADS=$SLURM_CPUS_PER_TASK

uv run python scripts/02_calidad.py

echo "=== [02_calidad] Fin: $(date) ==="
