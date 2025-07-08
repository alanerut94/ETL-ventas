import pandas as pd
from google.cloud import bigquery

def cargar_y_analizar_csv(ruta_csv, table_id):
    # Leer CSV
    df = pd.read_csv(ruta_csv)

    # Transformación simple: convertir fecha y calcular total
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["total"] = df["precio_unitario"] * df["cantidad"]

    # Análisis local: prendas más caras y vendidas
    prendas_mas_caras = (
        df.groupby("producto")["precio_unitario"]
        .max()
        .sort_values(ascending=False)
        .head(3)
    )
    prendas_mas_vendidas = (
        df.groupby("producto")["cantidad"]
        .sum()
        .sort_values(ascending=False)
        .head(3)
    )

    print("=== Análisis local (Python) ===")
    print("Prendas más caras:\n", prendas_mas_caras)
    print("\nPrendas más vendidas:\n", prendas_mas_vendidas)

    # Inicializar cliente BigQuery
    client = bigquery.Client()

    # Subir DataFrame a BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Esperar que termine
    print("\nDatos cargados exitosamente a BigQuery")

    # Consultas SQL en BigQuery
    query_caras = f"""
    SELECT producto, MAX(precio_unitario) AS precio_maximo
    FROM `{table_id}`
    GROUP BY producto
    ORDER BY precio_maximo DESC
    LIMIT 3
    """

    query_vendidas = f"""
    SELECT producto, SUM(cantidad) AS total_vendido
    FROM `{table_id}`
    GROUP BY producto
    ORDER BY total_vendido DESC
    LIMIT 3
    """

    # Ejecutar consultas y traer resultados
    df_caras = client.query(query_caras).to_dataframe()
    df_vendidas = client.query(query_vendidas).to_dataframe()

    print("\n=== Resultados desde BigQuery ===")
    print("Prendas más caras:\n", df_caras)
    print("\nPrendas más vendidas:\n", df_vendidas)

    # Guardar resultados en CSV locales
    df_caras.to_csv("prendas_mas_caras_bq.csv", index=False)
    df_vendidas.to_csv("prendas_mas_vendidas_bq.csv", index=False)
    print("\nResultados guardados en CSV locales.")

if __name__ == "__main__":
    # Cambia el path y el table_id a tus valores
    ruta_csv = r"C:\Users\alane\OneDrive\Desktop\proyecto\data\ventas_grande.csv"
    table_id = "proyecto-data-465200.ventas_dataset.ventas"

    cargar_y_analizar_csv(ruta_csv, table_id)
