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

#Crear tabla si es que no existe
create_table_departamentos = """
    CREATE TABLE IF NOT EXISTS departamentos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(255) NOT NULL
    )
    """

create_table_provincias = """
    CREATE TABLE IF NOT EXISTS provincias (
        id INT AUTO_INCREMENT PRIMARY KEY,
        departamento_id INT,
        nombre VARCHAR(255) NOT NULL,
        FOREIGN KEY (departamento_id) REFERENCES departamentos(id)
    )
    """

create_table_distritos = """
    CREATE TABLE IF NOT EXISTS distritos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ubigeo int UNIQUE,
        provincia_id INT,
        nombre VARCHAR(255) NOT NULL,
        poblacion INT NOT NULL,
        superficie FLOAT NOT NULL,
        y FLOAT NOT NULL,
        x FLOAT NOT null,
        FOREIGN KEY (provincia_id) REFERENCES provincias(id)
    )
    """

# Crear la tabla temporal
cursor.execute('''
    CREATE TEMPORARY TABLE temp_datos (
        ubigeo int,
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

cursor.execute(create_table_departamentos)

# Insertar los departamentos en la tabla departamento
cursor.execute('''
    INSERT INTO departamentos (nombre)
    SELECT DISTINCT departamento FROM temp_datos
''')


cursor.execute(create_table_provincias)
# Insertar las provincias en la tabla provincia
cursor.execute('''
    INSERT INTO provincias (nombre, departamento_id)
    SELECT DISTINCT temp_datos.provincia, departamentos.id
    FROM temp_datos
    JOIN departamentos ON temp_datos.departamento = departamentos.nombre
''')


cursor.execute(create_table_distritos)
# Insertar los distritos en la tabla distrito
cursor.execute('''
    INSERT INTO distritos (ubigeo, nombre, provincia_id, poblacion, superficie, x, y)
    SELECT temp_datos.ubigeo, temp_datos.distrito, provincias.id, temp_datos.poblacion, temp_datos.superficie, temp_datos.x, temp_datos.y
    FROM temp_datos
    JOIN provincias ON temp_datos.provincia = provincias.nombre
''')

conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()