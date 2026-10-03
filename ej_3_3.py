"""
Ejercicio 3.3 - Web Session Analysis.

Analiza sesiones de usuario a partir de datos de clickstream.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(event):
    """
    Extrae (usuario, página) de cada evento.

    Args:
        event: Diccionario con 'user', 'page' y 'time'

    Yields:
        Tupla (usuario, página)
    """
    yield (event['user'], event['page'])


def reducer(user, pages):
    """
    Calcula visitas totales y páginas únicas de un usuario.

    Args:
        user: Id del usuario
        pages: Lista de páginas visitadas (con repetidas)

    Returns:
        Diccionario con 'page_views' y 'unique_pages' (ordenadas)
    """
    return {
        'page_views': len(pages),
        'unique_pages': sorted(set(pages))
    }


if __name__ == "__main__":
    events = [
        {'user': 'U1', 'page': '/home', 'time': '10:00'},
        {'user': 'U1', 'page': '/products', 'time': '10:02'},
        {'user': 'U2', 'page': '/home', 'time': '10:01'},
        {'user': 'U1', 'page': '/cart', 'time': '10:05'},
        {'user': 'U2', 'page': '/about', 'time': '10:03'},
    ]

    if not events:
        print("No hay eventos para procesar.")
    else:
        results = mapreduce(events, mapper, reducer)
        for user, stats in results.items():
            print(f"{user}: {stats}")