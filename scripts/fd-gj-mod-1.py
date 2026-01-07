import sys
import os
import pandas as pd
import awswrangler as wr
import datetime
from awsglue.utils import getResolvedOptions


def obtener_index_metadata_periodo(col):
    for i, v in enumerate(col):
        if str(v).startswith("Mercado"):
            return i
    return None


def obtener_header(col):
    for i, v in enumerate(col):
        if str(v).startswith("Número"):
            return i
    return None


def validar_estructura(df):
    columnas = {
        'Número de operación', 'Fecha de la compra', 'Estado',
        'Descripción del estado', 'Cobro', 'Cargos e impuestos',
        'Anulaciones y reembolsos', 'Total a recibir',
        'Herramienta de cobro', 'Medio de pago', 'Descripción del ítem',
        'Cantidad', 'Local', 'Caja', 'Nombre de mi colaborador'
    }

    faltantes = columnas - set(df.columns)
    if faltantes:
        raise Exception(f"Columnas faltantes: {faltantes}")


def parsear_a_parquet(df, bucket, file_key):
    destino = file_key.replace("raw/", "validated/").rsplit(".", 1)[0] + ".parquet"
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{destino}",
        dataset=True
    )


def validar_main(metadata, df, bucket, file_key):
    validar_estructura(df)
    parsear_a_parquet(df, bucket, file_key)


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["file_key"])
    bucket = "franq-data"
    file_key = args["file_key"]

    path = f"s3://{bucket}/{file_key}"
    print(f"Procesando: {path}")

    df_temp = wr.s3.read_excel(path=path, header=None)

    metadata_col = df_temp.iloc[:, 0]
    header = obtener_header(metadata_col)

    if header is None:
        raise Exception("Header no encontrado")

    df = wr.s3.read_excel(path=path, header=header)

    validar_main(metadata_col, df, bucket, file_key)

    print("✅ Archivo validado y convertido a Parquet")
