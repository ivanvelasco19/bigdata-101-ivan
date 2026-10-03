# Ejercicios MapReduce en Python puro

Solución de los ejercicios de `EXERCISES.md` del módulo `01-mapreduce/01-pure-python`
del repo del curso ([camilo.soto/bigdata-101](https://gitlab.com/camilo.soto/bigdata-101)).
Todos usan el framework original (`mapreduce_framework.py`) sin modificarlo; en cada
ejercicio solo defino el `mapper` y el `reducer`.

## Cómo correrlos

Este repo contiene solo mis ejercicios. Para que corran, la carpeta tiene que ir dentro
del repo del curso, en:

```
bigdata-101/modules/01-mapreduce/01-pure-python/01-basics/mis_ejercicios/
```

porque los scripts importan `mapreduce_framework.py` (de `01-basics`), usan
`parallel_mapreduce.py` y `distributed_mapreduce.py` (de `03-distributed-simulation`)
y leen los datos de `datasets/`.

Una vez ubicada ahí, desde la carpeta `mis_ejercicios`:

```powershell
python ej_1_1.py
```

Los de nivel 2 y 3 que leen archivos buscan los datos solos en `datasets/`, y también
aceptan una ruta como argumento.

## Resumen de ejercicios

| Archivo | Ejercicio | Clave → Reducer | Resultado |
|---|---|---|---|
| `ej_1_1.py` | Contador de letras | letra → `sum` | `a: 6`, `e: 6` (ver nota 1) |
| `ej_1_2.py` | Palabras de más de 5 letras | palabra → `sum` | 9 palabras, `mapreduce: 3` |
| `ej_1_3.py` | Promedio de ventas por producto | producto → promedio | Laptop 1183.3, Mouse 27.5, Keyboard 77.5 |
| `ej_1_4.py` | Min, max y promedio por ciudad | ciudad → dict | Medellín: min 22, max 24, avg 23.0 |
| `ej_1_5.py` | Conteo, total y promedio por categoría | categoría → dict | Electronics: 3 / 1675 / 558.3 |
| `ej_2_1.py` | Distribución de longitudes | longitud → `sum` | 175 palabras; la longitud más común es 3 |
| `ej_2_2.py` | Palabras únicas por archivo | archivo → `len(set)` | sample_bigdata: 79, sample_text: 116 |
| `ej_2_3.py` | Índice invertido | palabra → archivos únicos | 177 palabras, 18 en ambos archivos |
| `ej_3_1.py` | Top-N palabras | palabra → `sum` + orden | a (10), the (9), of (8) |
| `ej_3_2.py` | Bigramas | par de palabras → `sum` | `"the quick": 2` |
| `ej_3_3.py` | Sesiones web | usuario → dict | U1: 3 visitas, U2: 2 visitas |
| `ej_3_4.py` | Detector de anomalías | sensor → dict | Regla 2σ: nada; MAD: `[99.9]` (ver nota 2) |
| `ej_p_1.py` | Secuencial vs paralelo | — | El paralelo pierde (ver nota 3) |
| `ej_p_2.py` | Efecto del tamaño de bloque | — | Ver nota 4 |

## Observaciones

**1. Error en la salida esperada del 1.1.** El enunciado dice `'a': 5` y `'e': 4`, pero
contando a mano da 6 y 6 (por ejemplo, la *a*: m**a**preduce, **a**, progr**a**mming,
l**a**rge, d**a**t**a**). La `c` y la `d` sí coinciden.

**2. La regla de 2σ no detecta el outlier del 3.4.** Con n = 3 y desviación poblacional,
el z-score máximo posible es (n−1)/√n ≈ 1.15, así que ningún valor puede superar 2σ. El
propio 99.9 infla la desviación estándar (36.37) y se esconde. Por eso agregué una
versión robusta con mediana y MAD (z-score modificado > 3.5), que sí lo detecta.

**3. Secuencial vs paralelo (P.1).** Wordcount sobre *The story of the universe*:

| Escenario | Líneas | Secuencial (s) | Paralelo (s) | Speedup |
|---|---|---|---|---|
| Libro x1 | 11,992 | 0.077 | 1.293 | 0.06x |
| Libro x10 | 119,920 | 0.748 | 2.064 | 0.36x |

Los dos dan el mismo resultado. El paralelo pierde porque arrancar procesos en Windows
tiene un costo fijo alto, y porque el map de wordcount es muy barato frente al costo de
mover más de 1.2 millones de pares `(palabra, 1)` entre procesos. Paralelizar solo paga
cuando el cómputo por registro pesa más que mover los datos; en Hadoop esto se mitiga
con un *combiner* que agrega localmente antes del shuffle.

**4. Tamaño de bloque (P.2).** Mismo libro (730.4 KB) en el HDFS simulado:

| Bloque | Bloques | Subida (s) | MapReduce (s) | Palabras únicas |
|---|---|---|---|---|
| 4 KB | 183 | 0.508 | 0.949 | 13,696 |
| 16 KB | 46 | 0.092 | 0.981 | 13,696 |
| 64 KB | 12 | 0.016 | 0.905 | 13,696 |

Los bloques pequeños multiplican archivos y réplicas, y la subida es unas 30 veces más
lenta con 4 KB que con 64 KB. El tiempo de MapReduce casi no cambia porque esta
simulación junta todos los bloques y siempre usa 4 mappers. En Hadoop real habría una
tarea de map por bloque, por eso HDFS usa bloques de 128 MB.
