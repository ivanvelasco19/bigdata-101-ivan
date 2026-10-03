"""
Ejercicio 1.2 - Long Words.

WordCount que solo cuenta palabras de más de 5 caracteres.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce, print_results


def mapper(line):
    """
    Emite (palabra, 1) solo para palabras de más de 5 caracteres.

    Args:
        line: Línea de texto

    Yields:
        Tuplas (palabra, 1) para palabras con 6 o más letras
    """
    for word in line.lower().split():
        word = word.strip('.,!?;:"()[]{}')
        if len(word) > 5:
            yield (word, 1)


def reducer(word, counts):
    """
    Suma las apariciones de una palabra.

    Args:
        word: La palabra
        counts: Lista de unos

    Returns:
        Total de apariciones de la palabra
    """
    return sum(counts)


if __name__ == "__main__":
    text = [
        "MapReduce is a programming model",
        "MapReduce processes large volumes of data",
        "The MapReduce model has two main phases",
        "The Map phase transforms the data",
        "The Reduce phase aggregates the results"
    ]

    if not text:
        print("No hay datos para procesar.")
    else:
        results = mapreduce(text, mapper, reducer)
        print_results(results, "Long Words (> 5 chars)")