"""
Ejercicio P.2 - Block Size Effect.

Mide cómo el tamaño de bloque del HDFS simulado afecta
el número de bloques y el tiempo de ejecución.
"""

import sys
import io
import time
import math
import shutil
import contextlib
from pathlib import Path

HERE = Path(__file__).resolve()
DISTRIBUTED = HERE.parents[2] / "03-distributed-simulation"
ROOT = HERE.parents[5]                                     # raíz del repo
sys.path.append(str(DISTRIBUTED))

from simulated_hdfs import SimulatedHDFS
from distributed_mapreduce import (distributed_mapreduce_from_hdfs,
                                   word_mapper, word_reducer)

BLOCK_SIZES = [4096, 16384, 65536]   # 4KB, 16KB, 64KB


def run_experiment(book, block_size):
    """
    Sube el libro a un HDFS nuevo con un tamaño de bloque dado y lo procesa.

    Args:
        book: Ruta del archivo de texto
        block_size: Tamaño de bloque en bytes

    Returns:
        Tupla (num_bloques, t_subida, t_mapreduce, palabras_unicas)
    """
    base_dir = HERE.parent / f"hdfs_p2_{block_size}"
    shutil.rmtree(base_dir, ignore_errors=True)   # arranca siempre limpio

    silent = io.StringIO()
    with contextlib.redirect_stdout(silent):
        hdfs = SimulatedHDFS(base_dir=str(base_dir), block_size=block_size,
                             replication=3, num_nodes=6)

        start = time.time()
        hdfs.put(str(book), "/data/book.txt")
        upload_time = time.time() - start

        num_blocks = len(hdfs.get_blocks("/data/book.txt"))

        start = time.time()
        results = distributed_mapreduce_from_hdfs(
            hdfs, "/data/book.txt", word_mapper, word_reducer,
            num_mappers=4, num_reducers=2)
        mr_time = time.time() - start

    shutil.rmtree(base_dir, ignore_errors=True)   # limpia al terminar
    return num_blocks, upload_time, mr_time, len(results)


if __name__ == "__main__":
    book_dir = ROOT / "datasets" / "book"
    books = sorted(book_dir.glob("*.txt"))

    if not books:
        print(f"No hay libros en: {book_dir}")
        sys.exit(1)

    book = books[0]
    size = book.stat().st_size
    if size == 0:
        print("El archivo está vacío.")
        sys.exit(1)

    print(f"Archivo: {book.name} ({size / 1024:.1f} KB)\n")
    print(f"{'Bloque':>8}{'Bloques':>9}{'Esperado':>10}"
          f"{'Subida (s)':>12}{'MapRed (s)':>12}{'Total (s)':>11}{'Únicas':>8}")
    print("-" * 70)

    for bs in BLOCK_SIZES:
        n, t_up, t_mr, uniq = run_experiment(book, bs)
        expected = math.ceil(size / bs)
        print(f"{bs // 1024:>6}KB{n:>9}{expected:>10}"
              f"{t_up:>12.3f}{t_mr:>12.3f}{t_up + t_mr:>11.3f}{uniq:>8}")