import sys
import logging
from script import validar_main
import os
from dotenv import load_dotenv

# --- CONFIGURACIÓN DE LOGS ---
logger = logging.getLogger(__name__)

# Agregamos %(funcName)s al formato para no tener que import inspect en cada funcion
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    stream=sys.stdout
)

if __name__ == "__main__":
    logger.info("INICIO del Glue Job")

    load_dotenv()

    file_key_arg: str = os.getenv("FILE_KEY")

    if not file_key_arg:
        raise ValueError("El parámetro 'file_key' es obligatorio.")

    validar_main(file_key_arg)

    logger.info("✅ FINALIZADO - Archivo validado y convertido a Parquet")
