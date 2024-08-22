DROP DATABASE Proyecto_ciudades;

create DATABASE Proyecto_ciudades;

use proyecto_ciudades;

CREATE TABLE departamento (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255) UNIQUE
);

CREATE TABLE provincia (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255),
    id_departamento INT,
    FOREIGN KEY (id_departamento) REFERENCES departamento(id)
);

CREATE TABLE distrito (
    ubigeo VARCHAR(6) PRIMARY KEY,
    nombre VARCHAR(255),
    id_provincia INT,
    poblacion BIGINT,
    superficie FLOAT,
    x FLOAT,
    y FLOAT,
    FOREIGN KEY (id_provincia) REFERENCES provincia(id)
);

show tables;
select * from distrito where nombre= 'guadalupe';