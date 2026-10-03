"""
Ejercicio P.1 - Sequential vs Parallel.

Compara el tiempo de WordCount con el framework secuencial
y con parallel_mapreduce sobre los libros de datasets/book.
"""

import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve()
BASICS = HERE.parents[1]                                   # 01-basics
DISTRIBUTED = HERE.parents[2] / "03-distributed-simulation"
ROOT = HERE.parents[5]                                     # raíz del repo
sys.path.append(str(BASICS))
sys.path.append(str(DISTRIBUTED))

from mapreduce_framework import mapreduce
from parallel_mapreduce import parallel_mapreduce, word_mapper, word_reducer


def load_lines(book_dir):
    """
    Lee todas las líneas no vacías de los .txt de un directorio.

    Args:
        book_dir: Directorio con los libros

    Returns:
        Lista de líneas
    """
    lines = []
    for file in sorted(book_dir.glob("*.txt")):
        print(f"Leyendo: {file.name}")
        with open(file, encoding="utf-8") as f:
            lines.extend(line.strip() for line in f if line.strip())
    return lines


def compare(data, label):
    """
    Corre WordCount secuencial y paralelo sobre los mismos datos y compara.

    Args:
        data: Lista de líneas
        label: Nombre del escenario para el reporte

    Returns:
        Tupla (tiempo_secuencial, tiempo_paralelo)
    """
    start = time.time()
    seq_results = mapreduce(data, word_mapper, word_reducer)
    seq_time = time.time() - start

    start = time.time()
    par_results = parallel_mapreduce(data, word_mapper, word_reducer,
                                     num_mappers=4, num_reducers=2)
    par_time = time.time() - start

    same = seq_results == par_results
    print(f"--- {label}: {len(data)} líneas ---")
    print(f"Sequential: {seq_time:.3f}s")
    print(f"Parallel:   {par_time:.3f}s")
    print(f"Speedup:    {seq_time / par_time:.2f}x")
    print(f"Mismos resultados: {same}\n")
    return seq_time, par_time


if __name__ == "__main__":
    book_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "datasets" / "book"

    if not book_dir.is_dir():
        print(f"No se encontró el directorio: {book_dir}")
        sys.exit(1)

    lines = load_lines(book_dir)
    if not lines:
        print("No hay texto para procesar.")
        sys.exit(1)

    summary = []
    # Escenario 1: el libro tal cual. Escenario 2: el libro repetido 10 veces
    for factor in (1, 10):
        data = lines * factor
        label = "Libro x1" if factor == 1 else f"Libro x{factor}"
        summary.append((label, len(data), *compare(data, label)))

    print("=" * 50)
    print(f"{'Escenario':<12}{'Líneas':>10}{'Seq (s)':>10}{'Par (s)':>10}")
    for label, n, seq_t, par_t in summary:
        print(f"{label:<12}{n:>10}{seq_t:>10.3f}{par_t:>10.3f}")