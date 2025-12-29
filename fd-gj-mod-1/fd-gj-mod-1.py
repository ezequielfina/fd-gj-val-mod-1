import sys
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# 1. Recuperar parámetros
args = getResolvedOptions(sys.argv, ['file_key'])
bucket = "franq-bucket"
file_key = args['file_key']
path = f"s3://{bucket}/{file_key}"

# 2. Lista de encodings a probar (de más común a menos)
encodings_to_try = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
df = None

print(f"Iniciando procesamiento de: {path}")

for enc in encodings_to_try:
    try:
        print(f"Intentando leer con encoding: {enc}")
        df = wr.s3.read_csv(path=path, encoding=enc)
        print(f"✅ Éxito al leer con {enc}")
        break  # Si tiene éxito, salimos del bucle
    except UnicodeDecodeError:
        print(f"❌ Falló con {enc}, intentando el siguiente...")
        continue
    except Exception as e:
        print(f"Error inesperado: {e}")
        raise

# 3. Validar si pudimos cargar algo
if df is None:
    raise Exception(f"No se pudo decodificar el archivo {file_key}. Ninguno de los encodings funcionó.")

if df.empty:
    raise Exception("El archivo está vacío")

# 4. Lógica de limpieza (Opcional pero recomendada)
# Eliminar caracteres extraños en los nombres de las columnas
df.columns = [col.encode('ascii', 'ignore').decode('ascii') for col in df.columns]

print(f"Archivo cargado correctamente. Filas: {len(df)}")

# 5. Guardar en la capa 'transformed' (Normalizado a Parquet para ahorrar costos)
# Esto hará que nunca más tengas problemas de encoding en el futuro
target_path = f"s3://{bucket}/transformed/{file_key.replace('.csv', '.parquet')}"
wr.s3.to_parquet(df=df, path=target_path, index=False)
print(f"Archivo normalizado guardado en: {target_path}")
