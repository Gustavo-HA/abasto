"""
03_baseline.py — Pronóstico base y simulación FIFO de reabasto.

Input  : outputs/ventas_clean.parquet
         data/CIMAT_BaseDatos.xlsx  (hoja Inventario, hoja CatSku)
Output : outputs/resultados_baseline.parquet

Lógica:
  PRONÓSTICO_BASE = 1.0 + promedio ventas últimas 4 semanas
  PEDIDO = max(0, PRONÓSTICO - INVENTARIO_DISPONIBLE)
  PEDIDO_REDONDEADO = ceil(PEDIDO / tamano_surtido) × tamano_surtido
  Inventario se actualiza cada semana (FIFO implícito: consume lo más antiguo primero)
  Merma = unidades que vencen sin venderse (tiempo_vida en semanas)
"""

import logging
import math
import sys
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "outputs"

INPUT_PATH = OUTPUT_DIR / "ventas_clean.parquet"
EXCEL_PATH = DATA_DIR / "CIMAT_BaseDatos.xlsx"
OUTPUT_PATH = OUTPUT_DIR / "resultados_baseline.parquet"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "03_baseline.log"),
    ],
)
log = logging.getLogger(__name__)

VENTANA_PRONOSTICO = 4   # semanas históricas para el promedio
N_SEMANAS_SIM = 12       # semanas a simular hacia el futuro


def calcular_pronostico_base(df: pl.DataFrame) -> pl.DataFrame:
    """
    Agrega PRONOSTICO_BASE = 1.0 + media móvil de las últimas VENTANA_PRONOSTICO semanas.
    Opera sobre el histórico completo; el pronóstico de la semana t usa t-4..t-1.
    """
    return (
        df.sort(["tienda_id", "articulo_id", "semana"])
        .with_columns(
            (
                1.0
                + pl.col("ventas_semana")
                .shift(1)
                .rolling_mean(window_size=VENTANA_PRONOSTICO)
                .over(["tienda_id", "articulo_id"])
            ).alias("pronostico_base")
        )
        # Rellenar NaN de las primeras semanas con 1.0 (mínimo operativo)
        .with_columns(
            pl.col("pronostico_base").fill_nan(1.0).fill_null(1.0)
        )
    )


def simular_reabasto(
    df_hist: pl.DataFrame,
    inv_inicial: pl.DataFrame,
    cat_sku: pl.DataFrame,
) -> pl.DataFrame:
    """
    Simulación semana a semana de las últimas N_SEMANAS_SIM del histórico.
    Devuelve un DataFrame con columnas de pedido, inventario, merma y utilidad.
    """
    # Obtener las últimas N_SEMANAS_SIM semanas disponibles
    semanas = sorted(df_hist["semana"].unique().to_list())
    semanas_sim = semanas[-N_SEMANAS_SIM:]

    # Catálogo útil: tamano_surtido, tiempo_vida, precio, costo
    sku_info = {
        row["articulo_id"]: row
        for row in cat_sku.iter_rows(named=True)
    }

    # Inventario inicial: {(tienda_id, articulo_id): inventario}
    inv = {
        (r["tienda_id"], r["articulo_id"]): r["inventario_inicial"]
        for r in inv_inicial.iter_rows(named=True)
    }

    # Pronóstico precalculado para las semanas de simulación
    pronosticos = {
        (r["tienda_id"], r["articulo_id"], r["semana"]): r["pronostico_base"]
        for r in df_hist.filter(pl.col("semana").is_in(semanas_sim)).iter_rows(named=True)
    }

    ventas_reales = {
        (r["tienda_id"], r["articulo_id"], r["semana"]): r["ventas_semana"]
        for r in df_hist.filter(pl.col("semana").is_in(semanas_sim)).iter_rows(named=True)
    }

    pares = df_hist.select(["tienda_id", "articulo_id"]).unique().iter_rows()
    registros = []

    for tienda_id, articulo_id in pares:
        info = sku_info.get(articulo_id, {})
        tamano = info.get("tamano_surtido", 1) or 1
        vida_sem = max(1, math.ceil((info.get("tiempo_vida", 7) or 7) / 7))
        precio = info.get("precio", 0) or 0
        costo = info.get("costo", 0) or 0

        # Cola FIFO: lista de (cantidad, semanas_restantes_de_vida)
        fifo: list[list] = []
        inv_actual = inv.get((tienda_id, articulo_id), 0)
        if inv_actual > 0:
            fifo.append([inv_actual, vida_sem])

        for semana in semanas_sim:
            key = (tienda_id, articulo_id, semana)
            pronostico = pronosticos.get(key, 1.0)
            venta_real = ventas_reales.get(key, 0)

            # --- Pedido ---
            inv_disponible = sum(q for q, _ in fifo)
            pedido_raw = max(0.0, pronostico - inv_disponible)
            pedido = math.ceil(pedido_raw / tamano) * tamano

            # Surtir pedido al inicio de la semana (llega el lunes)
            if pedido > 0:
                fifo.append([pedido, vida_sem])

            # --- Consumo (ventas reales) ---
            restante = float(venta_real)
            ventas_efectivas = 0.0
            while restante > 0 and fifo:
                lote_q, lote_vida = fifo[0]
                consumo = min(lote_q, restante)
                fifo[0][0] -= consumo
                restante -= consumo
                ventas_efectivas += consumo
                if fifo[0][0] <= 0:
                    fifo.pop(0)

            # --- Merma: decrementar vida y retirar vencidos ---
            merma = 0.0
            nuevos_fifo = []
            for lote_q, lote_vida in fifo:
                lote_vida -= 1
                if lote_vida <= 0:
                    merma += lote_q
                else:
                    nuevos_fifo.append([lote_q, lote_vida])
            fifo = nuevos_fifo

            inv_final = sum(q for q, _ in fifo)
            utilidad = ventas_efectivas * (precio - costo) - merma * costo

            registros.append(
                {
                    "tienda_id": tienda_id,
                    "articulo_id": articulo_id,
                    "semana": semana,
                    "pronostico": round(pronostico, 2),
                    "pedido": pedido,
                    "ventas_reales": venta_real,
                    "ventas_efectivas": ventas_efectivas,
                    "merma": merma,
                    "inventario_final": inv_final,
                    "utilidad": round(utilidad, 2),
                }
            )

    return pl.DataFrame(registros)


def main() -> None:
    if OUTPUT_PATH.exists():
        log.info("Output ya existe: %s — omitiendo recálculo.", OUTPUT_PATH)
        return

    if not INPUT_PATH.exists():
        log.error("Input no encontrado: %s. Corre primero 02_calidad.py", INPUT_PATH)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Cargar datos
    # ------------------------------------------------------------------
    log.info("Cargando %s ...", INPUT_PATH)
    df = pl.read_parquet(INPUT_PATH)
    log.info("Filas: %d", len(df))

    log.info("Cargando catálogos...")
    cat_sku = pl.read_excel(str(EXCEL_PATH), sheet_name="CatSku").rename({
        "Sku": "articulo_id",
        "Precio": "precio",
        "Costo": "costo",
        "TiempoVida": "tiempo_vida",
        "TamañoSurtido": "tamano_surtido",
    })
    inv_inicial = pl.read_excel(str(EXCEL_PATH), sheet_name="Inventario").rename({
        "Loc": "tienda_id",
        "Sku": "articulo_id",
        "Inventario": "inventario_inicial",
    })

    # ------------------------------------------------------------------
    # 2. Pronóstico base
    # ------------------------------------------------------------------
    log.info("Calculando pronóstico base (ventana=%d semanas)...", VENTANA_PRONOSTICO)
    df = calcular_pronostico_base(df)

    # ------------------------------------------------------------------
    # 3. Simulación
    # ------------------------------------------------------------------
    log.info(
        "Simulando reabasto FIFO para las últimas %d semanas...", N_SEMANAS_SIM
    )
    df_resultado = simular_reabasto(df, inv_inicial, cat_sku)

    # ------------------------------------------------------------------
    # 4. Métricas globales
    # ------------------------------------------------------------------
    utilidad_total = df_resultado["utilidad"].sum()
    ventas_total = df_resultado["ventas_efectivas"].sum()
    merma_total = df_resultado["merma"].sum()
    log.info("--- Resultados baseline ---")
    log.info("Utilidad total:  %.2f", utilidad_total)
    log.info("Ventas efectivas: %.0f unidades", ventas_total)
    log.info("Merma total:      %.0f unidades", merma_total)
    log.info(
        "Fill rate (ventas/demanda): %.2f%%",
        100 * ventas_total / max(1, df_resultado["ventas_reales"].sum()),
    )

    # ------------------------------------------------------------------
    # 5. Guardar
    # ------------------------------------------------------------------
    log.info("Guardando en %s ...", OUTPUT_PATH)
    df_resultado.write_parquet(OUTPUT_PATH, compression="zstd")
    log.info("Listo. Filas guardadas: %d", len(df_resultado))


if __name__ == "__main__":
    main()
