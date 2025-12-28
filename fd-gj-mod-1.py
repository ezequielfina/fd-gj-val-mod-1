import sys
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# 1. Recuperar el parámetro que viene de la Step Function
args = getResolvedOptions(sys.argv, ['file_key'])
bucket = "franq-bucket"
file_key = args['file_key']

# 2. Leer el archivo
path = f"s3://{bucket}/{file_key}"
df = wr.s3.read_csv(path=path)

# 3. Tu lógica de validación
if df.empty:
    raise Exception("El archivo está vacío")

print(f"Archivo {file_key} cargado con {len(df)} filas.")
