"""
Ejercicio 2.1 - Word Length Distribution.

Cuenta cuántas palabras hay de cada longitud en un archivo de texto.
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
    Emite (longitud, 1) por cada palabra de la línea.

    Args:
        line: Línea de texto

    Yields:
        Tuplas (longitud_de_la_palabra, 1)
    """
    for word in line.split():
        word = word.strip('.,!?;:"()[]{}')
        if word:
            yield (len(word), 1)


def reducer(length, counts):
    """
    Suma cuántas palabras tienen una longitud dada.

    Args:
        length: Longitud de palabra
        counts: Lista de unos

    Returns:
        Total de palabras con esa longitud
    """
    return sum(counts)


if __name__ == "__main__":
    # Permite pasar otro archivo por terminal; si no, usa sample_text.txt
    file_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_FILE

    if not file_path.exists():
        print(f"No se encontró el archivo: {file_path}")
        sys.exit(1)

    with open(file_path, encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        print("El archivo está vacío.")
    else:
        results = mapreduce(lines, mapper, reducer)
        print(f"Archivo: {file_path.name}\n")
        for length, count in sorted(results.items()):
            print(f"{length}: {count} words")