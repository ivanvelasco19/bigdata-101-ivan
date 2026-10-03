"""
Ejercicio 1.1 - Character Counter.

Cuenta la frecuencia de cada letra (solo letras, sin distinguir mayúsculas).
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(line):
    """
    Emite (letra, 1) por cada letra de la línea.

    Args:
        line: Línea de texto

    Yields:
        Tuplas (letra, 1), ignorando espacios, números y signos
    """
    for char in line.lower():
        if char.isalpha():
            yield (char, 1)


def reducer(char, counts):
    """
    Suma las apariciones de una letra.

    Args:
        char: La letra
        counts: Lista de unos

    Returns:
        Total de apariciones de la letra
    """
    return sum(counts)


if __name__ == "__main__":
    text = [
        "MapReduce is a programming model",
        "for processing large data sets",
    ]

    if not text:
        print("No hay datos para procesar.")
    else:
        results = mapreduce(text, mapper, reducer)
        print(dict(sorted(results.items())))