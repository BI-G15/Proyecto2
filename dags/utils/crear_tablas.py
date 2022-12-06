def crear_tablas_violencia():
    return """

        CREATE TABLE IF NOT EXISTS mineria_violencia(
            idMin VARCHAR(100) PRIMARY KEY,
            recurso VARCHAR(100),
			cantidad VARCHAR(100),
			unidades VARCHAR(100)
        );
		
        CREATE TABLE IF NOT EXISTS educacion_violencia(
            idEdu VARCHAR(100) PRIMARY KEY,
            coberturaNeta DECIMAL
        );
		
        CREATE TABLE IF NOT EXISTS demografia_violencia(
            idDem VARCHAR(100) PRIMARY KEY,
            cantPersonas DECIMAL
        );
		
        CREATE TABLE IF NOT EXISTS violencia_violencia(
            idVio VARCHAR(100) PRIMARY KEY,
            cSecuestros DECIMAL,
			cDesplazadas DECIMAL,
			cDesplaExp DECIMAL,
            cHomicidios DECIMAL
        );

        CREATE TABLE IF NOT EXISTS municipio_violencia(
            id VARCHAR(100) PRIMARY KEY,
            nombre VARCHAR(100),
            departamento VARCHAR(100),
            anio INT,
			idMin VARCHAR(100) REFERENCES mineria_violencia (idMin),
			idEdu VARCHAR(100) REFERENCES educacion_violencia (idEdu),
			idDem VARCHAR(100) REFERENCES demografia_violencia (idDem),
			idVio VARCHAR(100) REFERENCES violencia_violencia (idVio)
        );
    """

def crear_tablas_mujeres():
    return """
    
        CREATE TABLE IF NOT EXISTS mineria_mujeres(
            idMin VARCHAR(100) PRIMARY KEY,
            recurso VARCHAR(100),
			cantidad VARCHAR(100),
			unidades VARCHAR(100)
        );
		
        CREATE TABLE IF NOT EXISTS educacion_mujeres(
            idEdu VARCHAR(100) PRIMARY KEY,
            coberturaNeta DECIMAL
        );
		
        CREATE TABLE IF NOT EXISTS demografia_mujeres(
            idDem VARCHAR(100) PRIMARY KEY,
            cantMujeres DECIMAL,
			porcentajeMujeres DECIMAL
        );
		
        CREATE TABLE IF NOT EXISTS violencia_mujeres(
            idVio VARCHAR(100) PRIMARY KEY,
            homiMujer DECIMAL,
			violenciaMuj DECIMAL,
			tasaVioMuj DECIMAL
        );

        CREATE TABLE IF NOT EXISTS municipio_mujeres(
            id VARCHAR(100) PRIMARY KEY,
            nombre VARCHAR(100),
            departamento VARCHAR(100),
            anio INT,
			idMin VARCHAR(100) REFERENCES mineria_mujeres (idMin),
			idEdu VARCHAR(100) REFERENCES educacion_mujeres (idEdu),
			idDem VARCHAR(100) REFERENCES demografia_mujeres (idDem),
			idVio VARCHAR(100) REFERENCES violencia_mujeres (idVio)
        
        );
    """