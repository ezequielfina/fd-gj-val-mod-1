from typing import Optional
import sys
import pandas as pd
import awswrangler as wr
import logging

# --- CONFIGURACIÓN DE LOGS ---
logger = logging.getLogger(__name__)

# Agregamos %(funcName)s al formato para no tener que import inspect en cada funcion
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    stream=sys.stdout
)

# Constante para el bucket (o podría venir de args)
BUCKET_NAME = "franq-bucket"


def obtener_index_metadata_periodo(col: pd.Series) -> Optional[int]:
    # El logger ya sabe que función es por la config de arriba
    for i, v in enumerate(col):
        if str(v).startswith("Mercado"):
            return i
    return None


def obtener_header(col: pd.Series) -> Optional[int]:
    for i, v in enumerate(col):
        if str(v).startswith("Número"):
            return i
    return None


def validar_metadata_periodo(metadata: str, file_key: str) -> bool:
    """
    Valida período vs nombre del archivo.
    Maneja casos donde la metadata llega como NaN (float) o vacía.
    """

    meses = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }

    try:
        # 1. Validación de Nulos (Defensa contra NaN/Floats)
        if pd.isna(metadata):
            logger.warning(f"La celda de metadata es NULL/NaN en archivo: {file_key}")
            return False

        # 2. Forzar conversión a string (Defensa contra números puros)
        metadata_str = str(metadata)

        # 3. Validación de contenido mínimo
        # Si al convertir a string queda vacío o es muy corto, no seguimos
        if not metadata_str.strip():
            logger.warning("La metadata está vacía (string en blanco).")
            return False

        # 4. Procesamiento
        # Usamos metadata_str en lugar de metadata original
        if '\n' not in metadata_str:
            logger.warning(f"Formato incorrecto: No se encontraron saltos de línea en metadata: {metadata_str[:20]}...")
            return False

        lista_metadata_mp: list[str] = metadata_str.replace('\r', '').split('\n')[1].split()

        # Validación de longitud de la lista parseada
        if len(lista_metadata_mp) < 11:
            logger.warning(f"Metadata con formato inesperado (tokens insuficientes): {lista_metadata_mp}")
            return False

        fecha_inicio_str = f"{lista_metadata_mp[3]}-{meses[lista_metadata_mp[4][0:3].lower()]}-{lista_metadata_mp[5]}"
        fecha_fin_str = f"{lista_metadata_mp[8]}-{meses[lista_metadata_mp[9][0:3].lower()]}-{lista_metadata_mp[10]}"

        fecha_inicio = pd.to_datetime(fecha_inicio_str, dayfirst=True)
        fecha_fin = pd.to_datetime(fecha_fin_str, dayfirst=True)

        # Extracción de fecha del nombre del archivo
        # Asume formato raw/year=YYYY/month=MM/archivo.xls
        parts = file_key.split('/')

        # Agregamos try/except index por si la ruta no tiene el formato esperado
        try:
            year_file = parts[2].replace('year=', '')
            month_file = parts[3].replace('month=', '')
            periodo_file = pd.to_datetime(f'01-{month_file}-{year_file}', dayfirst=True)
        except IndexError:
            logger.error(f"La estructura de carpetas de file_key no es la esperada: {file_key}")
            return False

        coincide = (fecha_inicio.month == fecha_fin.month == periodo_file.month and
                    fecha_inicio.year == fecha_fin.year == periodo_file.year)

        if not coincide:
            logger.warning(f"Desajuste de fechas: Metadata({fecha_inicio.date()}) vs Archivo({periodo_file.date()})")

        return coincide

    except Exception as e:
        # El log te mostrará el tipo de dato real que causó el problema
        logger.error(
            f"Error crítico parseando metadata. Valor recibido: '{metadata}' (Tipo: {type(metadata)}). Error: {e}")
        return False


def validar_estructura(df: pd.DataFrame) -> bool:
    columnas_esperadas = {
        'Número de operación', 'Fecha de la compra', 'Estado',
        'Descripción del estado', 'Cobro', 'Cargos e impuestos',
        'Anulaciones y reembolsos', 'Total a recibir',
        'Herramienta de cobro', 'Medio de pago', 'Descripción del ítem',
        'Cantidad', 'Local', 'Caja', 'Nombre de mi colaborador'
    }

    faltantes = columnas_esperadas - set(df.columns)
    if faltantes:
        # Aquí lanzamos excepción porque si falta una columna, el proceso NO puede seguir
        raise ValueError(f"Estructura inválida. Columnas faltantes: {faltantes}")

    return True


def parsear_a_parquet(df: pd.DataFrame, file_key: str):
    # Generamos la ruta de salida dinámicamente
    destino: str = file_key.replace("raw/", "validated/").rsplit(".", 1)[0] + ".parquet"
    path_salida: str = f"s3://{BUCKET_NAME}/{destino}"

    logger.info(f"Escribiendo parquet en: {path_salida}")

    wr.s3.to_parquet(
        df=df,
        path=path_salida,
        dataset=True
    )


def leer_excel_s3(path_excel: str) -> pd.DataFrame:
    # Wrapper seguro para leer Excel
    try:
        logger.info(f"Leyendo archivo: {path_excel}")
        df: pd.DataFrame = wr.s3.read_excel(path_excel, header=None)

        if df.empty:
            raise ValueError(f"El archivo en {path_excel} está vacío.")
        return df

    except wr.exceptions.NoFilesFound:
        logger.error(f"No existe el archivo: {path_excel}")
        raise
    except Exception as e:
        logger.error(f"Error leyendo Excel: {e}")
        raise


def validar_main(file_key: str):
    path: str = f"s3://{BUCKET_NAME}/{file_key}"

    # 1. Primera lectura (Raw) para metadata
    df_temp = leer_excel_s3(path_excel=path)

    metadata_col = df_temp.iloc[:, 0]
    index_metadata = obtener_index_metadata_periodo(metadata_col)
    header_idx = obtener_header(metadata_col)

    if index_metadata is None or header_idx is None:
        raise ValueError("No se pudo determinar el Header o la Metadata del archivo.")

    metadata_text = df_temp.iloc[index_metadata, 0]

    # 2. Segunda lectura (Con Header correcto)
    # Nota: Es ineficiente leer 2 veces, pero necesario si el header es dinámico.
    df_con_header = wr.s3.read_excel(path=path, header=header_idx)

    # 3. Validaciones de Negocio
    if not validar_metadata_periodo(metadata_text, file_key):
        raise ValueError("El período del archivo no coincide con la metadata interna.")

    validar_estructura(df=df_con_header)  # Lanza excepción si falla

    # 4. Escritura
    parsear_a_parquet(df=df_con_header, file_key=file_key)


if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    logger.info("INICIO del Glue Job")

    args = getResolvedOptions(sys.argv, ["file_key"])
    file_key_arg = args["file_key"].strip()

    if not file_key_arg:
        raise ValueError("El parámetro 'file_key' es obligatorio.")

    validar_main(file_key_arg)

    logger.info("✅ FINALIZADO - Archivo validado y convertido a Parquet")
