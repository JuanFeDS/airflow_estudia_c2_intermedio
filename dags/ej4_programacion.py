"""Ejercicio 4: que el análisis corra solo (con un hueco para completar)."""
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="ej4_programacion",
    schedule="0 7 1-7 * 1",  # TODO: cambia None por una programación (por ejemplo "0 7 * * 1")
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ejercicio"],
)
def programacion():
    """Un análisis mensual muy sencillo, para practicar la programación."""

    @task
    def avisar():
        """Avisa que el análisis corrió."""
        print("Corrió el análisis de Lucía")

    avisar()


programacion()
