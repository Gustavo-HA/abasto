#!/bin/bash
#SBATCH --partition=GPU
#SBATCH --job-name=abasto_01_exploracion
#SBATCH --output=outputs/slurm_%j_01_exploracion.out
#SBATCH --error=outputs/slurm_%j_01_exploracion.err
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --mem=0
#SBATCH --time=03:00:00

# Enviar correo electrónico cuando el trabajo finalice o falle
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=gustavo.angeles@cimat.mx

# add uv to path
export PATH="/home/est_posgrado_gustavo.angeles/.local/uv/bin:$PATH"

set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== [01_exploracion] Inicio: $(date) ==="
echo "Nodo: $SLURMD_NODENAME"
echo "CPUs asignados: $SLURM_CPUS_PER_TASK"

# Polars puede usar todos los CPUs disponibles
export POLARS_MAX_THREADS=$SLURM_CPUS_PER_TASK

uv run python scripts/01_exploracion.py

echo "=== [01_exploracion] Fin: $(date) ==="
