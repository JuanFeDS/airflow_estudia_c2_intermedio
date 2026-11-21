"""Ejercicio 3: que el flujo avise cuando algo está raro (con un hueco para completar)."""
from datetime import datetime

from airflow.sdk import dag, task
from utilidades_lucia import CARPETA_DATOS, CARPETA_SALIDA, contar_repetidos, escribir_csv, leer_csv


@dag(
    dag_id="ej3_validacion",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ejercicio"],
)
def validacion():
    """Trae el archivo de abril, lo limpia y luego valida que no queden repetidos."""

    @task
    def traer():
        """Copia el archivo original de abril sin cambiarlo."""
        filas = leer_csv(CARPETA_DATOS / "prestamos_abril.csv")
        escribir_csv(CARPETA_SALIDA / "abril_bruto.csv", filas)
        print(f"Traje {len(filas)} filas")
        return len(filas)

    @task
    def limpiar():
        """Quita las filas repetidas y guarda el archivo limpio."""
        filas = leer_csv(CARPETA_SALIDA / "abril_bruto.csv")
        vistos = set()
        limpias = []
        for fila in filas:
            if fila["prestamo_id"] not in vistos:
                vistos.add(fila["prestamo_id"])
                limpias.append(fila)
        ruta_limpia = CARPETA_SALIDA / "abril_limpio.csv"
        escribir_csv(ruta_limpia, limpias)
        print(f"Quedaron {len(limpias)} filas limpias")
        return str(ruta_limpia)

    @task
    def validar_sin_repetidos(ruta_archivo):
        """Debe fallar si el archivo trae filas con el mismo prestamo_id."""
        filas = leer_csv(ruta_archivo)
        repetidas = contar_repetidos(filas, "prestamo_id")
        if repetidas > 0:
            raise ValueError(
                f"Se encontraron {repetidas} filas repetidas con prestamo_id duplicado en {ruta_archivo}."
            )
        print(f"Validación OK en {ruta_archivo}: {len(filas)} filas sin repetidos ({repetidas} repetidas).")

    t_traer = traer()
    t_limpiar = limpiar()
    t_validar = validar_sin_repetidos(t_limpiar)

    t_traer >> t_limpiar >> t_validar


validacion()