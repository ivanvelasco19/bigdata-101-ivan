"""
Ejercicio 1.5 - Count by Category.

Cuenta ventas por categoría y calcula total y promedio.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(sale):
    """
    Extrae (categoría, monto) de cada venta.

    Args:
        sale: Diccionario con 'product', 'category' y 'amount'

    Yields:
        Tupla (categoría, monto)
    """
    yield (sale['category'], sale['amount'])


def reducer(category, amounts):
    """
    Calcula conteo, total y promedio de ventas de una categoría.

    Args:
        category: Nombre de la categoría
        amounts: Lista de montos de venta

    Returns:
        Diccionario con 'count', 'total' y 'avg' (redondeado a 1 decimal)
    """
    return {
        'count': len(amounts),
        'total': sum(amounts),
        'avg': round(sum(amounts) / len(amounts), 1)
    }


if __name__ == "__main__":
    sales = [
        {'product': 'Laptop', 'category': 'Electronics', 'amount': 1200},
        {'product': 'Mouse', 'category': 'Electronics', 'amount': 25},
        {'product': 'Desk', 'category': 'Furniture', 'amount': 600},
        {'product': 'Chair', 'category': 'Furniture', 'amount': 350},
        {'product': 'Monitor', 'category': 'Electronics', 'amount': 450},
    ]

    if not sales:
        print("No hay ventas para procesar.")
    else:
        results = mapreduce(sales, mapper, reducer)
        for category, stats in results.items():
            print(f"{category}: {stats}")