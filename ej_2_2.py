"""
Ejercicio 2.2 - Unique Words per File.

Cuenta cuántas palabras únicas tiene cada archivo de un directorio.
"""

import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce

# Raíz del repo: sube 5 niveles desde mis_ejercicios
ROOT = Path(__file__).resolve().parents[5]
DEFAULT_DIR = ROOT / "datasets" / "mapreduce"


def mapper(file_data):
    """
    Emite (archivo, palabra) por cada palabra de la línea.

    Args:
        file_data: Tupla (nombre_archivo, línea)

    Yields:
        Tuplas (nombre_archivo, palabra en minúscula)
    """
    filename, line = file_data
    for word in line.lower().split():
        word = word.strip('.,!?;:"()[]{}')
        if word:
            yield (filename, word)


def reducer(filename, words):
    """
    Cuenta las palabras distintas de un archivo.

    Args:
        filename: Nombre del archivo
        words: Lista de todas las palabras del archivo (con repetidas)

    Returns:
        Número de palabras únicas
    """
    return len(set(words))


if __name__ == "__main__":
    # Permite pasar otro directorio por terminal; si no, usa datasets/mapreduce
    directory = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DIR

    if not directory.is_dir():
        print(f"No se encontró el directorio: {directory}")
        sys.exit(1)

    files = sorted(directory.glob("*.txt"))
    if not files:
        print("No hay archivos .txt en el directorio.")
        sys.exit(1)

    # Arma la lista de (archivo, línea) para todos los archivos
    data = []
    for file in files:
        with open(file, encoding="utf-8") as f:
            for line in f:
                data.append((file.name, line))

    results = mapreduce(data, mapper, reducer)

    for file in files:
        # Si un archivo está vacío no aparece en results, entonces queda en 0
        print(f"{file.name}: {results.get(file.name, 0)} unique words")