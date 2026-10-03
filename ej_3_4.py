"""
Ejercicio 3.4 - Anomaly Detector.

Detecta valores atípicos por sensor con dos criterios:
  1. Regla del enunciado: |valor - promedio| > 2 desviaciones estándar
  2. Versión robusta: z-score modificado con mediana y MAD > 3.5
"""

import sys
import os
import math
import statistics
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(reading):
    """
    Extrae (sensor, valor) de cada lectura.

    Args:
        reading: Diccionario con 'sensor' y 'value'

    Yields:
        Tupla (sensor, valor)
    """
    yield (reading['sensor'], reading['value'])


def reducer(sensor, values):
    """
    Calcula promedio, desviación estándar y anomalías de un sensor.

    Args:
        sensor: Id del sensor
        values: Lista de lecturas

    Returns:
        Diccionario con 'avg', 'std_dev', 'anomalies_2sd' y 'anomalies_mad'
    """
    # Criterio 1: regla de 2 desviaciones estándar (poblacional)
    avg = sum(values) / len(values)
    variance = sum((v - avg) ** 2 for v in values) / len(values)
    std_dev = math.sqrt(variance)
    anomalies_2sd = [v for v in values if abs(v - avg) > 2 * std_dev]

    # Criterio 2: z-score modificado con mediana y MAD (robusto a outliers)
    median = statistics.median(values)
    mad = statistics.median([abs(v - median) for v in values])
    if mad == 0:
        # La mayoría de valores son iguales: atípico es lo que se aparte de la mediana
        anomalies_mad = [v for v in values if v != median]
    else:
        anomalies_mad = [v for v in values
                         if 0.6745 * abs(v - median) / mad > 3.5]

    return {
        'avg': round(avg, 2),
        'std_dev': round(std_dev, 2),
        'anomalies_2sd': anomalies_2sd,
        'anomalies_mad': anomalies_mad
    }


if __name__ == "__main__":
    readings = [
        {'sensor': 'S1', 'value': 22.5},
        {'sensor': 'S1', 'value': 23.0},
        {'sensor': 'S1', 'value': 99.9},  # <- Anomalía
        {'sensor': 'S2', 'value': 15.0},
        {'sensor': 'S2', 'value': 14.8},
    ]

    if not readings:
        print("No hay lecturas para procesar.")
    else:
        results = mapreduce(readings, mapper, reducer)
        for sensor, stats in results.items():
            print(f"{sensor}: {stats}")