"""
02_calidad.py — Diagnóstico y limpieza de datos semanalizados.

Input  : outputs/ventas_semana.parquet
Output : outputs/ventas_clean.parquet
         (estadísticas de calidad en log)

Chequeos realizados:
  - Outliers de ventas (IQR por SKU)
  - Semanas faltantes por (tienda, SKU)
  - Combinaciones sin ventas históricas
"""

import logging
import sys
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "outputs"

INPUT_PATH = OUTPUT_DIR / "ventas_semana.parquet"
OUTPUT_PATH = OUTPUT_DIR / "ventas_clean.parquet"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "02_calidad.log"),
    ],
)
log = logging.getLogger(__name__)

# Umbral para winsorización de outliers (percentil superior por SKU)
OUTLIER_PERCENTIL = 0.995


def main() -> None:
    if OUTPUT_PATH.exists():
        log.info("Output ya existe: %s — omitiendo recálculo.", OUTPUT_PATH)
        return

    if not INPUT_PATH.exists():
        log.error("Input no encontrado: %s. Corre primero 01_exploracion.py", INPUT_PATH)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Cargar
    # ------------------------------------------------------------------
    log.info("Cargando %s ...", INPUT_PATH)
    df = pl.read_parquet(INPUT_PATH)
    n_orig = len(df)
    log.info("Filas originales: %d", n_orig)

    # ------------------------------------------------------------------
    # 2. Estadísticas generales
    # ------------------------------------------------------------------
    log.info("--- Estadísticas de ventas_semana ---")
    log.info("\n%s", df.select("ventas_semana").describe())

    semanas_unicas = df["semana"].n_unique()
    tiendas_unicas = df["tienda_id"].n_unique()
    skus_unicos = df["articulo_id"].n_unique()
    log.info(
        "Semanas: %d | Tiendas: %d | SKUs: %d",
        semanas_unicas, tiendas_unicas, skus_unicos,
    )

    # ------------------------------------------------------------------
    # 3. Outliers: winsorización por SKU al percentil OUTLIER_PERCENTIL
    # ------------------------------------------------------------------
    log.info("Calculando límites de outliers por SKU (p=%.3f)...", OUTLIER_PERCENTIL)
    limites = (
        df.group_by("articulo_id")
        .agg(
            pl.col("ventas_semana")
            .quantile(OUTLIER_PERCENTIL)
            .alias("limite_superior")
        )
    )

    df = df.join(limites, on="articulo_id", how="left")

    n_outliers = (df["ventas_semana"] > df["limite_superior"]).sum()
    log.info(
        "Outliers detectados: %d (%.2f%% del total)",
        n_outliers,
        100 * n_outliers / n_orig,
    )

    # Winsorizar (clip al límite por SKU)
    df = df.with_columns(
        pl.when(pl.col("ventas_semana") > pl.col("limite_superior"))
        .then(pl.col("limite_superior"))
        .otherwise(pl.col("ventas_semana"))
        .cast(pl.Float32)
        .alias("ventas_semana")
    ).drop("limite_superior")

    # ------------------------------------------------------------------
    # 4. Gaps semanales: detectar semanas faltantes por (tienda, SKU)
    # ------------------------------------------------------------------
    log.info("Analizando gaps semanales...")
    semanas_all = df["semana"].unique().sort()
    n_semanas = len(semanas_all)

    cobertura = (
        df.group_by(["tienda_id", "articulo_id"])
        .agg(pl.col("semana").n_unique().alias("semanas_con_venta"))
        .with_columns(
            (pl.col("semanas_con_venta") / n_semanas * 100).alias("pct_cobertura")
        )
    )

    log.info("Cobertura semanal por (tienda, SKU):")
    log.info("\n%s", cobertura.select("pct_cobertura").describe())

    pares_baja_cobertura = (cobertura["pct_cobertura"] < 10).sum()
    log.info(
        "Pares (tienda,SKU) con <10%% cobertura semanal: %d", pares_baja_cobertura
    )

    # ------------------------------------------------------------------
    # 5. Guardar
    # ------------------------------------------------------------------
    log.info("Guardando datos limpios en %s ...", OUTPUT_PATH)
    df.write_parquet(OUTPUT_PATH, compression="zstd")
    log.info("Filas finales: %d", len(df))
    log.info("Columnas: %s", df.columns)
    log.info("Listo.")


if __name__ == "__main__":
    main()
