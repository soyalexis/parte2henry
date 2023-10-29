from fastapi import FastAPI
import pandas as pd
import pyarrow
import fastparquet
app = FastAPI()

#http://127.0.0.1:8000
@app.get("/")
async def ruta_prueba():
    return "Hola"

url = 'C:\\Users\\W10\\Desktop\\henry data science\\proyecto individual 1\\PI MLOps - STEAM\\Entorno virtual 4\\Data\\df_primerafuncionv3parquet.parquet'
df = pd.read_parquet(url)

@app.get("/PlayTimeGenre/{genero}")
def PlayTimeGenre(genero: str):
    # Filtra las filas donde el género coincide
    df_filtered = df[df['genres'] == genero]

    if df_filtered.empty:
        return f"No se encontraron datos para el género '{genero}'"

    # Extrae el año de la columna 'Año' (asegúrate de que la columna se llame 'Año')
    df_filtered['Año'] = df_filtered['Año'].astype(int)

    # Encuentra el año con más horas jugadas para ese genero
    year_with_most_playtime = df_filtered.groupby('Año')['playtime_forever'].sum().idxmax()

    return {f"Año de lanzamiento con más horas jugadas para el género '{genero}'": year_with_most_playtime}