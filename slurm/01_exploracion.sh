#!/bin/bash
#SBATCH --job-name=abasto_01_exploracion
#SBATCH --output=outputs/slurm_%j_01_exploracion.out
#SBATCH --error=outputs/slurm_%j_01_exploracion.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=03:00:00

set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== [01_exploracion] Inicio: $(date) ==="
echo "Nodo: $SLURMD_NODENAME"
echo "CPUs asignados: $SLURM_CPUS_PER_TASK"

# Polars puede usar todos los CPUs disponibles
export POLARS_MAX_THREADS=$SLURM_CPUS_PER_TASK

uv run python scripts/01_exploracion.py

echo "=== [01_exploracion] Fin: $(date) ==="
