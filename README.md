# PROYECTO ABASTO
## Prompt maestro para IA (resolución de punta a punta)

Usa este documento como instrucción principal para que una IA resuelva el proyecto completo: entendimiento del problema, modelado, simulación, generación de entregables y recomendaciones ejecutivas.

---

## 1) Contexto de negocio

Optimizar el método de reabasto de productos perecederos en tienda para maximizar la utilidad del negocio.

**Alcance:**
- 50 productos
- 20,000 tiendas del norte del país
- Ventas diarias por artículo-tienda del 01/ENE/2017 al 23/JUN/2019

---

## 2) Función objetivo y reglas de negocio

Maximizar:

$$
	ext{UTILIDAD} = \text{UNIDADES VENDIDAS}\times(\text{PRECIO}-\text{COSTO}) - \text{UNIDADES MERMA}\times\text{COSTO}
$$

Metodología base de reabasto:

$$
	ext{PEDIDO} = \text{PRONÓSTICO} - \text{INVENTARIO}
$$

Si $\text{PEDIDO} < 0$, entonces $\text{PEDIDO} = 0$.

$$
	ext{PEDIDO\_REDONDEADO} = \left\lceil \frac{\text{PEDIDO}}{\text{TAMAÑO\_SURTIDO}} \right\rceil \times \text{TAMAÑO\_SURTIDO}
$$

Supuestos operativos:
- El inventario de tienda fue surtido el lunes previo.
- El reabasto ocurre cada lunes.
- El consumo de inventario sigue FIFO.
- Pronóstico base de referencia:

$$
	ext{PRONÓSTICO\_BASE} = 1.0 + \text{VENTA PROMEDIO SEMANAL DE LAS ÚLTIMAS 4 SEMANAS}
$$

Objetivo técnico: estimar el `INV_IDEAL` de cada lunes para las siguientes 12 semanas.

---

## 3) Estructura general del proyecto (sin cargar archivos pesados)

La IA debe asumir y trabajar con esta estructura:

```text
/
├── data/
│   └──   CIMAT_BaseDatos.xlsx    # bas de datos original
│   └── 00_Datos_Modelar.txt      # histórico principal de ventas e insumos
└── docs/
	└── proyecto_abasto.md        # prompt maestro y lineamientos del proyecto
```

> **Regla importante:** no leer el archivo completo en memoria de una sola vez. Trabajar por bloques/chunks o por particiones temporales.

---

## 4) Estructura de datos esperada (contrato lógico)

Como el archivo fuente es pesado, la IA debe tratar la tabla principal como un dataset transaccional diario por tienda-producto.

**Grano de la tabla:** `fecha` × `tienda_id` × `articulo_id`

**Columnas mínimas esperadas para resolver el proyecto:**
- `fecha` (date)
- `tienda_id` (string/int)
- `articulo_id` (string/int)
- `unidades_vendidas` (numérico)
- `precio` (numérico)
- `costo` (numérico)
- `tamano_surtido` (numérico entero)

**Columnas derivadas esperadas durante el proceso (no necesariamente en origen):**
- `inventario_inicial_semana`
- `pronostico_semana`
- `pedido`
- `pedido_redondeado`
- `merma_unidades`
- `utilidad_semana`
- `inv_ideal`

Si algunos nombres difieren en origen, la IA debe mapearlos explícitamente a este contrato lógico.

---

## 5) Tareas que la IA debe ejecutar de inicio a fin

1. Entender reglas de negocio y definir supuestos faltantes de forma explícita.
2. Validar calidad de datos (faltantes, duplicados, outliers, consistencia precio/costo/unidades).
3. Construir baseline con la regla de pronóstico base.
4. Diseñar y entrenar metodología mejorada de pronóstico/reabasto.
5. Simular 12 semanas hacia adelante con política semanal de lunes y FIFO.
6. Calcular utilidad comparativa: baseline vs propuesta.
7. Generar `INV_IDEAL` por tienda-producto-semana.
8. Preparar entregables ejecutivos (resumen, hallazgos, riesgos, recomendaciones).

---

## 6) Entregables requeridos

1. Presentación ejecutiva (metodología + comparación vs esquema base).
2. Tabla tipo Excel con `INV_IDEAL` para 12 semanas.
3. Cronograma de trabajo y cotización.
4. Recomendaciones generales para mejorar utilidad.

---

## 7) Criterios de evaluación

1. Calidad del formato y comunicación de resultados.
2. Solidez de la técnica estadística/analítica.
3. Desempeño en simulación con ventas reales de las 12 semanas futuras.

---

## 8) Instrucciones operativas para la IA

- Documentar cada supuesto y decisión técnica.
- Priorizar métodos robustos y escalables por volumen (50 productos × 20,000 tiendas).
- Evitar sobreajuste; reportar métricas de error y sensibilidad.
- Entregar resultados reproducibles (pasos, parámetros, tablas finales).
- Mantener trazabilidad de cómo se obtiene cada valor de `INV_IDEAL`.

