"""
Ejercicio 2.3 - Inverted Index.

Construye un índice invertido: para cada palabra, en qué archivos aparece.
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
    Emite (palabra, archivo) por cada palabra de la línea.

    Args:
        file_data: Tupla (nombre_archivo, línea)

    Yields:
        Tuplas (palabra en minúscula, nombre_archivo)
    """
    filename, line = file_data
    for word in line.lower().split():
        word = word.strip('.,!?;:"()[]{}')
        if word:
            yield (word, filename)


def reducer(word, filenames):
    """
    Devuelve la lista de archivos donde aparece una palabra.

    Args:
        word: La palabra
        filenames: Lista de archivos (con repetidos, uno por aparición)

    Returns:
        Lista ordenada de archivos sin repetir
    """
    return sorted(set(filenames))


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

    data = []
    for file in files:
        with open(file, encoding="utf-8") as f:
            for line in f:
                data.append((file.name, line))

    if not data:
        print("Los archivos están vacíos.")
        sys.exit(1)

    results = mapreduce(data, mapper, reducer)

    for word, filenames in sorted(results.items()):
        print(f"{word}: {filenames}")

    # Resumen
    in_all = [w for w, fs in results.items() if len(fs) == len(files)]
    print(f"\nTotal palabras indexadas: {len(results)}")
    print(f"Palabras presentes en todos los archivos: {len(in_all)}")