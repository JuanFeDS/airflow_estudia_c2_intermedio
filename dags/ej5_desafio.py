"""Ejercicio 5: desafío, dale un tercer control al flujo de Lucía."""
from datetime import datetime

from airflow.sdk import dag, task
from utilidades_lucia import CARPETA_DATOS, CARPETA_SALIDA, contar_repetidos, escribir_csv, leer_csv


@dag(
    dag_id="ej5_desafio",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ejercicio"],
)
def desafio():
    """Trae, limpia y revisa el archivo de abril con varios controles."""

    @task
    def traer():
        """Copia el archivo original de abril sin cambiarlo."""
        filas = leer_csv(CARPETA_DATOS / "prestamos_abril.csv")
        escribir_csv(CARPETA_SALIDA / "abril_bruto.csv", filas)
        return len(filas)

    @task
    def limpiar(filas_traidas):
        """Quita las filas repetidas y guarda el archivo limpio."""
        filas = leer_csv(CARPETA_SALIDA / "abril_bruto.csv")
        vistos = set()
        limpias = []
        for fila in filas:
            if fila["prestamo_id"] not in vistos:
                vistos.add(fila["prestamo_id"])
                limpias.append(fila)
        escribir_csv(CARPETA_SALIDA / "abril_limpio.csv", limpias)
        print(f"De {filas_traidas} filas quedaron {len(limpias)}")

    @task
    def control_sin_repetidos():
        """Control 1: no debe haber préstamos repetidos."""
        filas = leer_csv(CARPETA_SALIDA / "abril_limpio.csv")
        repetidas = contar_repetidos(filas, "prestamo_id")
        if repetidas:
            raise ValueError(f"Hay {repetidas} préstamos repetidos")
        print("Control 1 correcto: sin repetidos")

    @task
    def control_fechas():
        """Control 2: las fechas de préstamo deben leerse como dd/mm/aaaa."""
        filas = leer_csv(CARPETA_SALIDA / "abril_limpio.csv")
        malas = []
        for fila in filas:
            try:
                datetime.strptime(fila["fecha_prestamo"], "%d/%m/%Y")
            except ValueError:
                malas.append(fila["prestamo_id"])
        if malas:
            raise ValueError(f"{len(malas)} fechas no se pueden leer: {malas[:5]}")
        print("Control 2 correcto: todas las fechas se leen")

    @task
    def mi_control():
        """Control 3: todo libro_id en abril_limpio debe existir en datos/libros.csv."""
        prestamos = leer_csv(CARPETA_SALIDA / "abril_limpio.csv")
        libros = leer_csv(CARPETA_DATOS / "libros.csv")

        ids_libros_validos = {fila["libro_id"].strip() for fila in libros if fila.get("libro_id")}
        sin_libro = []

        for fila in prestamos:
            libro_id = (fila.get("libro_id") or "").strip()
            if not libro_id or libro_id not in ids_libros_validos:
                sin_libro.append((fila.get("prestamo_id"), libro_id))

        if sin_libro:
            raise ValueError(
                f"{len(sin_libro)} préstamos tienen libro_id inválido o inexistente: {sin_libro[:5]}"
            )
        print(f"Control 3 correcto: todos los {len(prestamos)} préstamos tienen libro_id válido")

    limpio = limpiar(traer())
    limpio >> [control_sin_repetidos(), control_fechas(), mi_control()]


desafio()


