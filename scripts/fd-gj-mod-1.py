import sys
import boto3
import os
import io
import pandas as pd
import awswrangler as wr
import datetime


def obtener_index_metadata_periodo(df_temp: pd.Series) -> int | None:
    if df_temp.empty:
        return None

    for i in range(len(df_temp)):
        valor = str(df_temp.iloc[i])
        if valor[0:7] == 'Mercado':
            return i
    return None


def obtener_header(df_temp: pd.Series):
    if df_temp.empty:
        return None

    for i in range(len(df_temp)):
        valor = str(df_temp.iloc[i])
        if valor[0:6] == 'Número':
            return i
    return None


def validar_estructura(df_data: pd.DataFrame) -> bool:
    col_necesarias = {'Número de operación',
                           'Fecha de la compra',
                           'Estado',
                           'Descripción del estado',
                           'Cobro',
                           'Cargos e impuestos',
                           'Anulaciones y reembolsos',
                           'Total a recibir',
                           'Herramienta de cobro',
                           'Medio de pago',
                           'Descripción del ítem',
                           'Cantidad',
                           'Local',
                           'Caja',
                           'Nombre de mi colaborador'}

    if df_data.empty:
        print('El dataframe está vacío')
        return False

    faltantes = [c for c in col_necesarias if c not in df_data.columns]
    if faltantes:
        print(f'Faltan columnas: {faltantes}')
        return False

    if df_data.count().sum() == 0:
        print(f'El archivo tiene filas, pero todas están vacías')
        return False

    return True


def parsear_a_parquet(df: pd.DataFrame, file_key: str):
    # 1. Obtener el bucket
    bucket = os.getenv('BUCKET')
    # 2. Cambiar la carpeta de 'raw' a 'validated'
    new_key = file_key.replace('raw/', 'validated/', 1)
    # 3. Cambiar cualquier extensión a '.parquet' usando rsplit
    # Esto asegura que solo cambies la extensión final
    clean_key = new_key.rsplit('.', 1)[0] + '.parquet'
    s3_new_path = f"s3://{bucket}/{clean_key}"

    wr.s3.to_parquet(
        df=df,
        path=s3_new_path,
        dataset=True
    )


def validar_metadata_periodo(metadata: str, file_key: str) -> (bool, int):
    """Validamos si los períodos de la metadata del archivo de MP coincide con el período marcado durante la carga"""
    import calendar

    meses = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }

    lista_metadata_mp: [] = metadata.split('\n')[1].split()

    fecha_inicio_metadata_mp_crudo = f"{lista_metadata_mp[3]}-{meses[lista_metadata_mp[4][0:3].lower()]}-{lista_metadata_mp[5]}"
    fecha_inicio_metadata_mp: datetime.datetime = pd.to_datetime(fecha_inicio_metadata_mp_crudo, dayfirst=True)

    fecha_fin_metadata_mp_crudo = f"{lista_metadata_mp[8]}-{meses[lista_metadata_mp[9][0:3].lower()]}-{lista_metadata_mp[10]}"
    fecha_fin_metadata_mp: datetime.datetime = pd.to_datetime(fecha_fin_metadata_mp_crudo, dayfirst=True)

    year_file_key: str = file_key.split('/')[2].replace('year=', '')
    month_file_key: str = file_key.split('/')[3].replace('month=', '')
    periodo_file_key: datetime.datetime = pd.to_datetime(f'01-{month_file_key}-{year_file_key}', dayfirst=True)
    _, ult_dia_mes_file_key = calendar.monthrange(periodo_file_key.year, periodo_file_key.month)

    return ((fecha_inicio_metadata_mp.month == fecha_fin_metadata_mp.month == periodo_file_key.month and
             fecha_inicio_metadata_mp.year == fecha_fin_metadata_mp.year == periodo_file_key.year)), periodo_file_key.year


def validar_main(metadata_periodo: str, df: pd.DataFrame, file_key: str) -> bool:
    if validar_metadata_periodo(metadata_periodo, file_key) and validar_estructura(df_data=df):
        parsear_a_parquet(df=df, file_key=file_key)
        return True
    return False


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    client: boto3.client = boto3.client('s3',
                                        aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                                        aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
                                        region_name=os.getenv('AWS_REGION'))

    obj = client.get_object(Bucket=os.getenv('BUCKET'), Key=os.getenv('FILE_KEY'))

    data = obj['Body'].read()
    df_temp = pd.read_excel(io.BytesIO(data), nrows=10, header=None)

    metadata_periodo = df_temp.iloc[:, 0]

    index_metadata_periodo: int = obtener_index_metadata_periodo(df_temp=metadata_periodo)
    header: int = obtener_header(df_temp=metadata_periodo)

    df = pd.read_excel(io.BytesIO(data), header=header)

    print(validar_main(
        metadata_periodo=metadata_periodo[index_metadata_periodo],
        df=df,
        file_key=os.getenv('FILE_KEY')
    ))

else:
    from awsglue.utils import getResolvedOptions

    # 1. Recuperar parámetros
    args = getResolvedOptions(sys.argv, ['file_key'])
    # OJO: Asegurate que este bucket sea el correcto o pásalo como argumento también
    bucket = "franq-bucket"
    file_key = args['file_key']
    path = f"s3://{bucket}/{file_key}"

    print(f"Iniciando procesamiento de: {path}")

    # ELIMINADO EL BUCLE DE ENCODINGS: read_excel lee binarios, no texto plano.
    try:
        # Pandas/Wrangler detectan automáticamente el formato xlsx
        df_temp = wr.s3.read_excel(path=path, header=None)
    except Exception as e:
        print(f"Error crítico leyendo el Excel: {e}")
        raise e

    if df_temp.empty:
        raise Exception("El archivo está vacío")

    metadata_periodo = df_temp.iloc[:, 0]
    index_metadata_periodo: int = obtener_index_metadata_periodo(df_temp=metadata_periodo)
    header: int = obtener_header(df_temp=metadata_periodo)

    # Volvemos a leer con el header correcto
    df = wr.s3.read_excel(path=path, header=header)

    # CORRECCIÓN CLAVE:
    es_valido = validar_main(
        metadata_periodo=metadata_periodo[index_metadata_periodo],
        df=df,
        file_key=file_key
    )

    if not es_valido:
        # Esto forzará que el Glue Job termine en estado FAILED
        raise Exception(f"VALIDACIÓN FALLIDA para el archivo {file_key}. Revise logs.")

    print(f"Validación exitosa. Archivo procesado.")
