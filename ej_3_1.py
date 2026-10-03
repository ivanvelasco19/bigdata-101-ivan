"""
Ejercicio 3.1 - Top-N Words.

Cuenta palabras con MapReduce y devuelve las N más frecuentes.
"""

import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce

# Raíz del repo: sube 5 niveles desde mis_ejercicios
ROOT = Path(__file__).resolve().parents[5]
DEFAULT_FILE = ROOT / "datasets" / "mapreduce" / "sample_text.txt"


def mapper(line):
    """
    Emite (palabra, 1) por cada palabra de la línea.

    Args:
        line: Línea de texto

    Yields:
        Tuplas (palabra en minúscula, 1)
    """
    for word in line.lower().split():
        word = word.strip('.,!?;:"()[]{}')
        if word:
            yield (word, 1)


def reducer(word, counts):
    """
    Suma las apariciones de una palabra.

    Args:
        word: La palabra
        counts: Lista de unos

    Returns:
        Total de apariciones
    """
    return sum(counts)


def top_n_words(text, n=10):
    """
    Devuelve las N palabras más frecuentes.

    Args:
        text: Lista de líneas
        n: Cantidad de palabras a devolver

    Returns:
        Lista de tuplas (palabra, conteo) ordenada de mayor a menor
    """
    if not text or n <= 0:
        return []
    results = mapreduce(text, mapper, reducer)
    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
    return sorted_results[:n]


if __name__ == "__main__":
    # Uso: python ej_3_1.py [archivo] [n]
    file_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_FILE
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    if not file_path.exists():
        print(f"No se encontró el archivo: {file_path}")
        sys.exit(1)

    with open(file_path, encoding="utf-8") as f:
        lines = f.readlines()

    top = top_n_words(lines, n)

    if not top:
        print("No hay palabras para mostrar.")
    else:
        print(f"Top {n} palabras en {file_path.name}:\n")
        for i, (word, count) in enumerate(top, start=1):
            print(f"{i:>2}. {word}: {count}")