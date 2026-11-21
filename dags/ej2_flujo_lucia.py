"""Ejercicio 2: el flujo de Lucía en tres pasos (con dos huecos para completar)."""
from datetime import datetime

from airflow.sdk import dag, task
from utilidades_lucia import CARPETA_DATOS, CARPETA_SALIDA, ID_DUNE, escribir_csv, leer_csv


@dag(
    dag_id="ej2_flujo_lucia",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ejercicio"],
)
def flujo_lucia():
    """Trae el archivo de abril, lo limpia y cuenta los préstamos de «Dune»."""

    @task
    def traer():
        """Copia el archivo original de abril sin cambiarlo."""
        filas = leer_csv(CARPETA_DATOS / "prestamos_abril.csv")
        escribir_csv(CARPETA_SALIDA / "abril_bruto.csv", filas)
        print(f"Traje {len(filas)} filas del archivo de abril")
        return len(filas)

    @task
    def limpiar(filas_traidas):
        """Prepara el archivo de abril para contar."""
        filas = leer_csv(CARPETA_SALIDA / "abril_bruto.csv")
        # TODO 1: quita las filas repetidas (las que tienen el mismo prestamo_id)
        vistos = set()
        filas_unicas = []
        for fila in filas:
            pid = fila["prestamo_id"]
            if pid not in vistos:
                vistos.add(pid)
                filas_unicas.append(fila)
        filas = filas_unicas
        escribir_csv(CARPETA_SALIDA / "abril_limpio.csv", filas)
        print(f"Tenía {filas_traidas} filas y quedaron {len(filas)}")
        return len(filas)

    @task
    def contar(filas_limpias):
        """Cuenta los préstamos de «Dune» en abril."""
        filas = leer_csv(CARPETA_SALIDA / "abril_limpio.csv")
        # TODO 2: cuenta las filas donde libro_id sea ID_DUNE e imprime el resultado
        total_dune = sum(1 for fila in filas if fila["libro_id"] == ID_DUNE)
        print(f"Revisé {filas_limpias} filas limpias y hay {total_dune} préstamos de «Dune» en abril")

    contar(limpiar(traer()))


flujo_lucia()
