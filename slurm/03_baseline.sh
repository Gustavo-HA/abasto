#!/bin/bash
#SBATCH --partition=GPU
#SBATCH --job-name=abasto_03_baseline
#SBATCH --output=outputs/slurm_%j_03_baseline.out
#SBATCH --error=outputs/slurm_%j_03_baseline.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --nodes=1
#SBATCH --mem=0
#SBATCH --time=02:00:00

# Enviar correo electrónico cuando el trabajo finalice o falle
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=gustavo.angeles@cimat.mx

# add uv to path
export PATH="/home/est_posgrado_gustavo.angeles/.local/uv/bin:$PATH"


set -euo pipefail

cd "$SLURM_SUBMIT_DIR"

echo "=== [03_baseline] Inicio: $(date) ==="
echo "Nodo: $SLURMD_NODENAME"

export POLARS_MAX_THREADS=$SLURM_CPUS_PER_TASK

uv run python scripts/03_baseline.py

echo "=== [03_baseline] Fin: $(date) ==="
