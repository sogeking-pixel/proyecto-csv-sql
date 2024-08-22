import pandas as pd
import mysql.connector

# Leer el archivo CSV
df = pd.read_csv('C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\geodir-ubigeo-reniec.csv')
df['poblacion'] = df['Poblacion'].str.replace(',', '').astype(int)
df['superficie'] = df['Superficie'].str.replace(',', '').astype(float)
df['y'] = df['Y'].astype(float)
df['x'] = df['X'].astype(float)

# Conectar a la base de datos MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='123456',
    database='proyecto_ciudades'
)

cursor = conn.cursor()

# Crear la tabla temporal
cursor.execute('''
    CREATE TEMPORARY TABLE temp_datos (
        ubigeo VARCHAR(6),
        distrito VARCHAR(255),
        provincia VARCHAR(255),
        departamento VARCHAR(255),
        poblacion INT,
        superficie FLOAT,
        y FLOAT,
        x FLOAT
    )
''')

# Insertar los datos en la tabla temporal
for _, row in df.iterrows():
    cursor.execute('''
        INSERT INTO temp_datos (ubigeo, distrito, provincia, departamento, poblacion, superficie, y, x)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (row['Ubigeo'], row['Distrito'], row['Provincia'], row['Departamento'], row['poblacion'], row['superficie'], row['Y'], row['X']))
    print(row['Ubigeo'], row['Distrito'], row['Provincia'], row['Departamento'], row['poblacion'], row['superficie'], row['Y'], row['X'])

conn.commit()

# Insertar los departamentos en la tabla departamento
cursor.execute('''
    INSERT INTO departamento (nombre)
    SELECT DISTINCT departamento FROM temp_datos
''')

# Insertar las provincias en la tabla provincia
cursor.execute('''
    INSERT INTO provincia (nombre, id_departamento)
    SELECT DISTINCT temp_datos.provincia, departamento.id
    FROM temp_datos
    JOIN departamento ON temp_datos.departamento = departamento.nombre
''')

# Insertar los distritos en la tabla distrito
cursor.execute('''
    INSERT INTO distrito (ubigeo, nombre, id_provincia, poblacion, superficie, x, y)
    SELECT temp_datos.ubigeo, temp_datos.distrito, provincia.id, temp_datos.poblacion, temp_datos.superficie, temp_datos.x, temp_datos.y
    FROM temp_datos
    JOIN provincia ON temp_datos.provincia = provincia.nombre
''')

conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()