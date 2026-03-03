"""
01_exploracion.py — Carga, semanalización y enriquecimiento del histórico de ventas.

Input  : data/00_Datos_Modelar.txt  (9 GB, separador |)
         data/CIMAT_BaseDatos.xlsx
Output : outputs/ventas_semana.parquet
"""

import logging
import sys
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

VENTAS_PATH = DATA_DIR / "00_Datos_Modelar.txt"
EXCEL_PATH = DATA_DIR / "CIMAT_BaseDatos.xlsx"
OUTPUT_PATH = OUTPUT_DIR / "ventas_semana.parquet"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "01_exploracion.log"),
    ],
)
log = logging.getLogger(__name__)


def main() -> None:
    if OUTPUT_PATH.exists():
        log.info("Output ya existe: %s — omitiendo recálculo.", OUTPUT_PATH)
        return

    # ------------------------------------------------------------------
    # 1. Cargar catálogo (pequeño, cabe en RAM)
    # ------------------------------------------------------------------
    log.info("Cargando catálogo desde %s", EXCEL_PATH)
    cat_sku = pl.read_excel(str(EXCEL_PATH), sheet_name="CatSku")
    cat_loc = pl.read_excel(str(EXCEL_PATH), sheet_name="CatLoc")
    inventario = pl.read_excel(str(EXCEL_PATH), sheet_name="Inventario")

    # Log columnas reales del Excel para detectar discrepancias de nombres
    log.info("CatSku columnas:   %s", cat_sku.columns)
    log.info("CatLoc columnas:   %s", cat_loc.columns)
    log.info("Inventario columnas: %s", inventario.columns)

    # Normalizar nombres de columnas del Excel a snake_case interno
    cat_sku = cat_sku.rename({
        "Sku": "articulo_id",
        "Precio": "precio",
        "Costo": "costo",
        "TiempoVida": "tiempo_vida",
        "TamañoSurtido": "tamano_surtido",
    })
    cat_loc = cat_loc.rename({
        "LOC": "tienda_id",
        "REGION": "region",
        "PLAZA": "plaza",
    })
    inventario = inventario.rename({
        "Loc": "tienda_id",
        "Sku": "articulo_id",
        "Inventario": "inventario_inicial",
    })

    log.info(
        "Catálogo cargado — SKUs: %d, Tiendas: %d, Inventarios: %d",
        len(cat_sku),
        len(cat_loc),
        len(inventario),
    )

    # ------------------------------------------------------------------
    # 2. Scan ventas (LazyFrame streaming, no carga todo en RAM)
    # ------------------------------------------------------------------
    log.info("Escaneando ventas: %s", VENTAS_PATH)
    lf = pl.scan_csv(
        VENTAS_PATH,
        separator="|",
        schema_overrides={
            "Loc": pl.Int32,
            "Sku": pl.Int32,
            "Fecha": pl.Date,
            "Uni": pl.Int32,
        },
    ).rename(
        {
            "Loc": "tienda_id",
            "Sku": "articulo_id",
            "Fecha": "fecha",
            "Uni": "unidades_vendidas",
        }
    )

    log.info("Schema: %s", lf.collect_schema())

    # ------------------------------------------------------------------
    # 3. Semanalización: agrupar por (tienda, SKU, lunes de la semana)
    # ------------------------------------------------------------------
    log.info("Agregando a semanas...")
    lf_semana = (
        lf.with_columns(
            # lunes de la semana ISO correspondiente
            (pl.col("fecha") - pl.duration(days=pl.col("fecha").dt.weekday())).alias(
                "semana"
            )
        )
        .group_by(["tienda_id", "articulo_id", "semana"])
        .agg(pl.col("unidades_vendidas").sum().alias("ventas_semana"))
        .sort(["tienda_id", "articulo_id", "semana"])
    )

    # ------------------------------------------------------------------
    # 4. Join con catálogo de SKUs y tiendas
    # ------------------------------------------------------------------
    log.info("Haciendo join con catálogo...")
    lf_enriquecido = (
        lf_semana.join(
            cat_sku.lazy(),
            on="articulo_id",
            how="left",
        ).join(
            cat_loc.lazy(),
            on="tienda_id",
            how="left",
        )
    )

    # ------------------------------------------------------------------
    # 5. Collect y guardar
    # ------------------------------------------------------------------
    log.info("Recolectando y guardando en %s ...", OUTPUT_PATH)
    df = lf_enriquecido.collect(engine="streaming")

    log.info(
        "Dataset semanal: %d filas, %d columnas", df.shape[0], df.shape[1]
    )
    log.info("Columnas: %s", df.columns)
    log.info("Rango semanas: %s → %s", df["semana"].min(), df["semana"].max())

    df.write_parquet(OUTPUT_PATH, compression="zstd")
    log.info("Guardado exitoso: %s", OUTPUT_PATH)


if __name__ == "__main__":
    main()
