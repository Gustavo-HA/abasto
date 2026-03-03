#!/bin/bash
# pipeline.sh — Encadena todos los jobs del pipeline de abasto con dependencias.
#
# Uso:
#   cd /ruta/al/proyecto/abasto
#   bash slurm/pipeline.sh
#
# Cada job sólo inicia si el anterior terminó exitosamente (afterok).

set -euo pipefail

cd "$(dirname "$0")/.."

echo "Submitting pipeline de abasto..."

JOB1=$(sbatch --parsable slurm/01_exploracion.sh)
echo "  [01_exploracion] Job ID: $JOB1"

JOB2=$(sbatch --parsable --dependency=afterok:$JOB1 slurm/02_calidad.sh)
echo "  [02_calidad]     Job ID: $JOB2  (depende de $JOB1)"

JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 slurm/03_baseline.sh)
echo "  [03_baseline]    Job ID: $JOB3  (depende de $JOB2)"

echo ""
echo "Pipeline enviado. Estado:"
echo "  squeue --jobs=$JOB1,$JOB2,$JOB3 --format='%.10i %.20j %.8T %.10M %.6D %R'"
squeue --jobs=$JOB1,$JOB2,$JOB3 --format="%.10i %.20j %.8T %.10M %.6D %R" 2>/dev/null || true
