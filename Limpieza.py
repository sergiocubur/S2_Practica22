# Importar librerías
from sqlite3 import IntegrityError
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Lectura Archivo remoto
ruta1 = "https://seminario2.blob.core.windows.net/fase1/global.csv?sp=r&st=2023-12-06T03:45:26Z&se=2024-01-04T11:45:26Z&sv=2022-11-02&sr=b&sig=xdx7LdUOekGyBvGL%2FNE55ZZj9SBvCC%2FWegxtpSsKjJg%3D"
df1 = pd.read_csv(ruta1)

# Lectura Archivo Local 
ruta2 = "municipio.csv"
df2 = pd.read_csv(ruta2)

# Combinar los DataFrames en una única tabla
combined_df = pd.concat([df1, df2], ignore_index=True)
print(combined_df)
combined_df = combined_df[combined_df['Country_code'] == 'GT']
# Limpiar los datos
# Aquí puedes realizar operaciones de limpieza según tus necesidades
# Por ejemplo, convertir la columna de fechas a formato datetime
combined_df['Date_reported'] = pd.to_datetime(combined_df['Date_reported'], errors='coerce')


# Filtrar datos del año 2022
#combined_df = combined_df[combined_df['Date_reported'].dt.year == 2022]

# Nombre de las tablas propuestas
fecha_table_name = 'Fecha'
pais_table_name = 'Pais'
informe_diario_table_name = 'InformeDiario'

# Conexión a la base de datos (reemplaza 'usuario', 'contraseña', 'localhost', 'nombre_de_base_de_datos')
engine = create_engine('mysql://root:@localhost/seminario2')

# Crear el DataFrame de la entidad "Fecha"
fecha_df = pd.DataFrame({'Date_reported': combined_df['Date_reported']})

# Crear el DataFrame de la entidad "País"
pais_df = combined_df[['Country_code', 'Country', 'WHO_region']].drop_duplicates()

# Crear el DataFrame de la entidad "InformeDiario"
informe_diario_df = combined_df[['New_cases', 'Cumulative_cases', 'New_deaths', 'Cumulative_deaths']]

# Crear relaciones
relacion_fecha_informe = combined_df[['Date_reported']].merge(informe_diario_df, left_index=True, right_index=True)
relacion_pais_informe = combined_df[['Country_code']].merge(informe_diario_df, left_index=True, right_index=True)

# Definir las tablas y relaciones a insertar
tables_to_insert = [
    (fecha_df, fecha_table_name),
    (pais_df, pais_table_name),
    (informe_diario_df, informe_diario_table_name),
    (relacion_fecha_informe, 'Relacion_Fecha_Informe'),
    (relacion_pais_informe, 'Relacion_Pais_Informe'),
]

fecha_df = fecha_df.dropna()
informe_diario_df = informe_diario_df.dropna()
fecha_df = fecha_df.dropna()
relacion_fecha_informe = relacion_fecha_informe.dropna()
relacion_pais_informe = relacion_pais_informe.dropna()

# Insertar en lotes de 50 registros
#batch_size = 50

#or df, table_name in tables_to_insert:
#    for i in range(0, len(df), batch_size):
#        batch = df.iloc[i:i + batch_size]
#        batch.to_sql(table_name, con=engine, if_exists='append', index=False)
#        print(f"Lote insertado en {table_name}: {i + 1} - {i + len(batch)} de {len(df)}")

#print("Inserción de lotes completada.")

# Consultar datos de las tablas
query_fecha = "SELECT * FROM Fecha"
query_informe = "SELECT * FROM InformeDiario"
query_pais = "SELECT * FROM Pais"
query_relacion_fecha_informe = "SELECT * FROM Relacion_Fecha_Informe"
query_relacion_pais_informe = "SELECT * FROM Relacion_Pais_Informe"

df_fecha = pd.read_sql(query_fecha, engine)
df_informe = pd.read_sql(query_informe, engine)
df_pais = pd.read_sql(query_pais, engine)
df_relacion_fecha_informe = pd.read_sql(query_relacion_fecha_informe, engine)
df_relacion_pais_informe = pd.read_sql(query_relacion_pais_informe, engine)

# Estadísticas descriptivas
estadisticas_informe = df_informe.describe()
estadisticas_pais = df_pais.describe()
estadisticas_fecha = df_fecha.describe()

# Histograma y diagrama de caja para InformeDiario
fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 8))

# Nuevos casos
sns.histplot(df_informe['New_cases'], kde=True, ax=axes[0, 0])
axes[0, 0].set_title('Histograma - Nuevos Casos')

# Casos acumulados
sns.histplot(df_informe['Cumulative_cases'], kde=True, ax=axes[0, 1])
axes[0, 1].set_title('Histograma - Casos Acumulados')

# Nuevas muertes
sns.histplot(df_informe['New_deaths'], kde=True, ax=axes[1, 0])
axes[1, 0].set_title('Histograma - Nuevas Muertes')

# Muertes acumuladas
sns.histplot(df_informe['Cumulative_deaths'], kde=True, ax=axes[1, 1])
axes[1, 1].set_title('Histograma - Muertes Acumuladas')

# Ajustes de diseño
plt.tight_layout()
plt.show()

# Diagrama de caja para la población de los municipios (tabla Pais)
plt.figure(figsize=(8, 6))
sns.boxplot(x=df_pais['Country_code'])
plt.title('Diagrama de Barras - Country Code de los Países')
plt.show()

# Imprimir estadísticas descriptivas
print("Estadísticas InformeDiario:")
print(estadisticas_informe)

print("\nEstadísticas Pais:")
print(estadisticas_pais)

print("\nEstadísticas Fecha:")
print(estadisticas_fecha)


##Datos Cualitativos
# Consulta SQL para obtener datos de la tabla `Pais`
sql_query_pais = "SELECT * FROM Pais"

# Ejecutar la consulta y cargar los resultados en un DataFrame
df_pais = pd.read_sql_query(sql_query_pais, engine)

# Conteo de registros por país
conteo_paises = df_pais['Country'].value_counts()

# Crear diagrama de barras para países
plt.figure(figsize=(12, 6))
conteo_paises.plot(kind='bar', color='lightgreen')
plt.title('Conteo de Registros por País')
plt.xlabel('País')
plt.ylabel('Número de Registros')
plt.show()

# Consultas SQL
query_relacion_fecha_informe = "SELECT * FROM Relacion_Fecha_Informe"
query_relacion_pais_informe = "SELECT * FROM Relacion_Pais_Informe"

# Ejecutar las consultas y cargar los resultados en DataFrames
df_relacion_fecha_informe = pd.read_sql(query_relacion_fecha_informe, con=engine)
df_relacion_pais_informe = pd.read_sql(query_relacion_pais_informe, con=engine)

# Gráficas de dispersión para datos cuantitativos
plt.figure(figsize=(15, 10))

# Fecha vs cantidad de nuevas muertes
plt.subplot(2, 2, 1)
sns.scatterplot(x='Date_reported', y='New_deaths', data=df_relacion_fecha_informe)
plt.title('Fecha vs Nuevas Muertes')

# País vs cantidad de nuevas muertes
plt.subplot(2, 2, 2)
sns.scatterplot(x='Country_code', y='New_deaths', data=df_relacion_pais_informe)
plt.title('País vs Nuevas Muertes')

# Fecha vs cantidad de muertes acumuladas
plt.subplot(2, 2, 3)
sns.scatterplot(x='Date_reported', y='Cumulative_deaths', data=df_relacion_fecha_informe)
plt.title('Fecha vs Muertes Acumuladas')

# País vs cantidad de muertes acumuladas
plt.subplot(2, 2, 4)
sns.scatterplot(x='Country_code', y='Cumulative_deaths', data=df_relacion_pais_informe)
plt.title('País vs Muertes Acumuladas')

plt.tight_layout()
plt.show()

# Gráficas de barras para datos cualitativos
plt.figure(figsize=(15, 5))

# Fecha vs cantidad de nuevas muertes
plt.subplot(1, 2, 1)
sns.barplot(x='Date_reported', y='New_deaths', data=df_relacion_fecha_informe)
plt.title('Fecha vs Nuevas Muertes')
plt.xticks(rotation=45, ha='right')

# País vs cantidad de nuevas muertes
plt.subplot(1, 2, 2)
sns.barplot(x='Country_code', y='New_deaths', data=df_relacion_pais_informe)
plt.title('País vs Nuevas Muertes')
plt.xticks(rotation=45, ha='right')

plt.tight_layout()
plt.show()


