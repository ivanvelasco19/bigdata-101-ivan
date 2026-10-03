"""
Ejercicio 1.4 - Temperature Statistics.

Calcula temperatura mínima, máxima y promedio por ciudad.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(record):
    """
    Extrae (ciudad, temperatura) de cada registro.

    Args:
        record: Diccionario con 'city', 'temperature' y 'date'

    Yields:
        Tupla (ciudad, temperatura)
    """
    yield (record['city'], record['temperature'])


def reducer(city, temperatures):
    """
    Calcula mínimo, máximo y promedio de temperatura de una ciudad.

    Args:
        city: Nombre de la ciudad
        temperatures: Lista de temperaturas

    Returns:
        Diccionario con 'min', 'max' y 'avg' (redondeado a 1 decimal)
    """
    return {
        'min': min(temperatures),
        'max': max(temperatures),
        'avg': round(sum(temperatures) / len(temperatures), 1)
    }


if __name__ == "__main__":
    temperatures = [
        {'city': 'Medellin', 'temperature': 22, 'date': '2026-01-01'},
        {'city': 'Bogota', 'temperature': 14, 'date': '2026-01-01'},
        {'city': 'Medellin', 'temperature': 24, 'date': '2026-01-02'},
        {'city': 'Cali', 'temperature': 28, 'date': '2026-01-01'},
        {'city': 'Bogota', 'temperature': 13, 'date': '2026-01-02'},
        {'city': 'Cali', 'temperature': 30, 'date': '2026-01-02'},
        {'city': 'Medellin', 'temperature': 23, 'date': '2026-01-03'},
        {'city': 'Bogota', 'temperature': 15, 'date': '2026-01-03'},
        {'city': 'Cartagena', 'temperature': 32, 'date': '2026-01-01'},
        {'city': 'Cartagena', 'temperature': 33, 'date': '2026-01-02'},
    ]

    if not temperatures:
        print("No hay registros para procesar.")
    else:
        results = mapreduce(temperatures, mapper, reducer)
        for city, stats in results.items():
            print(f"{city}: {stats}")