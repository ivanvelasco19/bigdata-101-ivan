"""
Ejercicio 1.3 - Average Sales per Product.

Calcula el monto promedio de venta por producto.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from mapreduce_framework import mapreduce


def mapper(sale):
    """
    Extrae (producto, monto) de cada venta.

    Args:
        sale: Diccionario con 'product' y 'amount'

    Yields:
        Tupla (producto, monto)
    """
    yield (sale['product'], sale['amount'])


def reducer(product, amounts):
    """
    Calcula el promedio de ventas de un producto.

    Args:
        product: Nombre del producto
        amounts: Lista de montos de venta

    Returns:
        Promedio redondeado a 1 decimal
    """
    return round(sum(amounts) / len(amounts), 1)


if __name__ == "__main__":
    sales = [
        {'product': 'Laptop', 'amount': 1200},
        {'product': 'Mouse', 'amount': 25},
        {'product': 'Laptop', 'amount': 1100},
        {'product': 'Mouse', 'amount': 30},
        {'product': 'Laptop', 'amount': 1250},
        {'product': 'Keyboard', 'amount': 75},
        {'product': 'Keyboard', 'amount': 80},
    ]

    if not sales:
        print("No hay ventas para procesar.")
    else:
        results = mapreduce(sales, mapper, reducer)
        for product, avg in results.items():
            print(f"{product}: {avg}")