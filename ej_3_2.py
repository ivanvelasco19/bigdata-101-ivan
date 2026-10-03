"""
Ejercicio 3.2 - Bigram Analysis.

Encuentra los pares de palabras consecutivas más comunes.
"""

import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(line):
    """
    Emite (bigrama, 1) por cada par de palabras consecutivas de la línea.

    Args:
        line: Línea de texto

    Yields:
        Tuplas ("palabra1 palabra2", 1)
    """
    words = [w.strip('.,!?;:"()[]{}') for w in line.lower().split()]
    words = [w for w in words if w]
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i + 1]}"
        yield (bigram, 1)


def reducer(bigram, counts):
    """
    Suma las apariciones de un bigrama.

    Args:
        bigram: Par de palabras
        counts: Lista de unos

    Returns:
        Total de apariciones del bigrama
    """
    return sum(counts)


if __name__ == "__main__":
    # Uso: python ej_3_2.py [archivo]  -> sin archivo usa el texto del enunciado
    if len(sys.argv) > 1:
        file_path = Path(sys.argv[1])
        if not file_path.exists():
            print(f"No se encontró el archivo: {file_path}")
            sys.exit(1)
        with open(file_path, encoding="utf-8") as f:
            text = f.readlines()
        source = file_path.name
    else:
        text = ["the quick brown fox", "the quick red dog"]
        source = "texto del enunciado"

    if not text:
        print("No hay datos para procesar.")
    else:
        results = mapreduce(text, mapper, reducer)
        top = sorted(results.items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"Bigramas más comunes ({source}):\n")
        for bigram, count in top:
            print(f'("{bigram}", {count})')