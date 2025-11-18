import sqlite3
import os
import sys

# 1. Añadir el directorio raíz del proyecto al path
current_dir = os.path.dirname(__file__)
# Subimos TRES niveles (desde /proceso_db/scripts/dim/ hasta el raíz)
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
sys.path.append(project_root)

# 2. Importar el gestor de configuración
from config_manager import obtener_ruta

try:
    # 3. Obtener la ruta desde config.json
    ruta_db = obtener_ruta("ruta_database")
except Exception as e:
    print(f"Error al cargar ruta_database desde config_manager: {e}")
    sys.exit(1)

# --- MAPAS CON JERARQUÍA ---

# Nivel 1: dim_VehicleType (Padre de Categoría)
map_vehicle_type = {
    1: 'Ligero',
    2: 'Pesado'
}

# Nivel 2: dim_Category
# Formato: (idCategory, CategoryName, idVehicleType_FK)
map_category = [
    (1, 'Moto', 1),                      # Ligero
    (2, 'Auto/Camioneta', 1),            # Ligero
    (3, 'Camión 2 Ejes Cta/Rd', 2),    # Pesado
    (4, 'Bus 2 Ejes', 2),                # Pesado
    (5, 'Camión +2 Ejes', 2),           # Pesado
    (6, 'Bus +2 Ejes', 2),               # Pesado
    (12, 'Sobredimensionado', 2)         # Pesado
]

# Nivel 3: dim_VehicleTypeValue
# Formato: (idVehicleTypeValue, VehicleTypeName, idCategory_FK)
map_vehicle_type_value = [
    (0, "Sin datos", None),
    (1, "Bus/Taxibus", 4), # Bus 2 Ejes
    (2, "Minibus", 4), # Bus 2 Ejes
    (4, "Automóvil", 2), # Auto/Camioneta
    (5, "Camioneta", 2), # Auto/Camioneta
    (6, "Jeep", 2), # Auto/Camioneta
    (7, "Camión simple", 3), # Camión 2 Ejes
    (8, "Camión c/remolque", 5), # Camión +2 Ejes
    (9, "Tracto-Camión", 3), # Camión 2 Ejes
    (10, "Tracto-Camión c/semirremolque", 5), # Camión +2 Ejes
    (11, "Furgón", 2), # Auto/Camioneta
    (14, "Motocicleta", 1), # Moto
    (15, "Bicicleta", None), # No aplica a peaje
    (16, "Tracción Animal", None),
    (12, "Ambulancia", 2), # Auto/Camioneta
    (13, "Carro Bomba", 3), # Camión 2 Ejes
    (17, "Maq. Agrícola", 12), # Sobredimensionado
    (18, "Maq. Mov. Tierra", 12), # Sobredimensionado
    (20, "Camión Pluma", 5) # Camión +2 Ejes
]

# ==========================================================
# --- MAPAS DE DIMENSIONES (TRÁFICO) ---
# ==========================================================

map_plaza = {
    1: 'Cachiyuyo',
    2: 'Punta Colorada'
}

map_direction = {
    1: 'ASCENDENTE',
    2: 'DESCENDENTE'
}


# ==========================================================
# --- MAPAS DE DIMENSIONES (FICHA 0 - ACCIDENTES) ---
# ==========================================================

map_accident_type = {
    0: 'Atropello', 10: 'Atropello', 20: 'Caida', 31: 'Colisión Frontal', 32: 'Colisión Lateral',
    33: 'Colisión por Alcance', 34: 'Colisión Perpendicular', 40: 'Impacto con Animal',
    51: 'Choque con objeto Frontal', 52: 'Choque con objeto Lateral', 53: 'Choque con objeto Posterior',
    61: 'Choque con otro vehículo detenido Frenet/Frente', 62: 'Choque con otro vehículo detenido Frenet/Lado',
    63: 'Choque con otro vehículo detenido Frenet/Posterior', 64: 'Choque con otro vehículo detenido Lado/Frente',
    65: 'Choque con otro vehículo detenido Lado/Lado', 66: 'Choque con otro vehículo detenido Lado/Posterior',
    67: 'Choque con otro vehículo detenido Posterior/Frente', 68: 'Choque con otro vehículo detenido Posterior/Lado',
    69: 'Choque con otro vehículo detenido Posterior/Posterior', 70: 'Volcadura', 80: 'Incendio',
    90: 'Descarrilamiento', 99: 'Otro Tipo'
}

map_relative_location = {
    0: 'Sin dato', 1: 'Tramo de vía recta', 2: 'Tramo de vía curva horizontal', 3: 'Tramo de vía curva vertical',
    4: 'Acera o berma', 5: 'Puente', 6: 'Túnel', 11: 'Cruce con semáforo funcionando',
    12: 'Cruce con semáforo apagado', 13: 'Cruce regulado por carabinero', 14: 'Cruce con señal PARE',
    15: 'Cruce con señal CEDA EL PASO', 16: 'Cruce sin señalización', 21: 'Enlace a nivel',
    22: 'Enlace a desnivel', 23: 'Acceso no habilitado', 24: 'Rotonda', 25: 'Plaza de peaje',
    99: 'Otros no considerados'
}

map_surface_condition = {
    0: 'Sin dato', 1: 'Seca', 2: 'Húmeda', 3: 'Mojada', 4: 'Con Barro',
    5: 'Con Nieve', 6: 'Con Aceite', 7: 'Escarcha', 8: 'Gravilla', 99: 'Otros'
}

map_luminosity = {
    0: 'Sin dato', 1: 'Diurna', 2: 'Nocturna', 3: 'Amanecer', 4: 'Atardecer'
}

map_weather = {
    0: 'Sin dato', 1: 'Despejado', 2: 'Nublado', 3: 'Lluvia', 4: 'Llovizna',
    5: 'Neblina', 6: 'Nieve'
}

map_artificial_light = {
    0: 'Sin dato', 1: 'Apagada', 2: 'Encendida suficiente', 3: 'Encendida insuficiente', 4: 'No existe'
}

map_section = {
    0: 'Sin dato', 1: 'Troncal', 2: 'Ramal', 3: 'Calle de Servicio', 4: 'Corredor', 5: 'Mixta'
}

map_lanes = {
    0: 'Sin dato', 1: 'Pista 1', 2: 'Pista 2', 3: 'Pista 3', 4: 'Pista 4', 5: 'Pista 5', 6: 'Pista 6'
}

map_environment = [
    ('Punto Duro', 0, 'Sin dato/No informado'), ('Punto Duro', 10, 'con defensa'), ('Punto Duro', 11, 'Sin defensa'), ('Punto Duro', 12, 'No existe'),
    ('Defensas Camineras', 0, 'Sin dato/No informado'), ('Defensas Camineras', 20, 'Mediana'), ('Defensas Camineras', 21, 'Lateral izquierda'), ('Defensas Camineras', 22, 'Lateral derecha'), ('Defensas Camineras', 23, 'No existe'),
    ('Desnivel en la Faja', 0, 'Sin dato/No informado'), ('Desnivel en la Faja', 30, 'Existe con Protecciones'), ('Desnivel en la Faja', 31, 'Existe sin Protecciones'), ('Desnivel en la Faja', 32, 'No Existe'),
    ('Estado cerco', 0, 'Sin dato/No informado'), ('Estado cerco', 40, 'Bueno'), ('Estado cerco', 41, 'Regular'), ('Estado cerco', 42, 'Malo'), ('Estado cerco', 43, 'N/A'),
    ('Trabajos en la Vía', 0, 'Sin dato/No informado'), ('Trabajos en la Vía', 50, 'si'), ('Trabajos en la Vía', 51, 'no'),
    ('Banderero', 0, 'Sin dato/No informado'), ('Banderero', 60, 'si'), ('Banderero', 61, 'no'),
    ('Velocidad máxima del sector', 0, 'Sin dato/No informado'), ('Velocidad máxima del sector', 70, '0-50'), ('Velocidad máxima del sector', 71, '50-80'), ('Velocidad máxima del sector', 72, '80-100'), ('Velocidad máxima del sector', 73, '100-120')
]

map_response = [
    ('Carabineros', 0, 'Sin dato/No informado'), ('Carabineros', 1, 'si'), ('Carabineros', 2, 'no'),
    ('Ambulancia', 0, 'Sin dato/No informado'), ('Ambulancia', 1, 'si'), ('Ambulancia', 2, 'no'),
    ('Bomberos', 0, 'Sin dato/No informado'), ('Bomberos', 1, 'si'), ('Bomberos', 2, 'no'),
    ('Operadora', 0, 'Sin dato/No informado'), ('Operadora', 1, 'si'), ('Operadora', 2, 'no'),
    ('ITE', 0, 'Sin dato/No informado'), ('ITE', 1, 'si'), ('ITE', 2, 'no')
]

map_consequence = ['Muertos', 'Graves', 'Menos Graves', 'Leves', 'Ilesos']
map_affected = ['Conductores', 'Pasajeros', 'Peatones', 'Sin identificar']

map_probable_cause = [
    ('Contratos de Corredores urbanos', 0, 'Sin dato'), ('Contratos de Corredores urbanos', 1, 'conducción bajo influencia del alcohol'), ('Contratos de Corredores urbanos', 4, 'conducción en condiciones físicas deficientes (cansancio, sueño)'),
    ('Contratos de Corredores urbanos', 5, 'fuga por hecho delictual'), ('Contratos de Corredores urbanos', 8, 'velocidad mayor que la máxima permitida'), ('Contratos de Corredores urbanos', 11, 'detención o disminución de velocidad intempestiva'),
    ('Contratos de Corredores urbanos', 19, 'no respetar el derecho a paso del peatón'), ('Contratos de Corredores urbanos', 26, 'no respetar señalización'), ('Contratos de Corredores urbanos', 28, 'conducción no atenta a las condiciones de tránsito del momento'),
    ('Contratos de Corredores urbanos', 31, 'conducción contra sentido del tránsito'), ('Contratos de Corredores urbanos', 32, 'virajes indebidos'), ('Contratos de Corredores urbanos', 38, 'cruce de peatón en zona no habilitada'),
    ('Contratos de Corredores urbanos', 39, 'peatón bajo la influencia del alcohol o en estado de ebriedad'), ('Contratos de Corredores urbanos', 40, 'semáforo apagado'), ('Contratos de Corredores urbanos', 41, 'Otra'),
    ('Falla humana', 0, 'Sin dato'), ('Falla humana', 1, 'si'), ('Falla humana', 2, 'no'),
    ('Falla mecánica', 0, 'Sin dato'), ('Falla mecánica', 1, 'si'), ('Falla mecánica', 2, 'no'),
    ('Reventón neumático', 0, 'Sin dato'), ('Reventón neumático', 1, 'si'), ('Reventón neumático', 2, 'no'),
    ('Peatón en la vía', 0, 'Sin dato'), ('Peatón en la vía', 1, 'si'), ('Peatón en la vía', 2, 'no'),
    ('Ciclista en la vía', 0, 'Sin dato'), ('Ciclista en la vía', 1, 'si'), ('Ciclista en la vía', 2, 'no'),
    ('Animal u obstactulo en la vía', 0, 'Sin dato'), ('Animal u obstactulo en la vía', 1, 'si'), ('Animal u obstactulo en la vía', 2, 'no'),
    ('Pavimento resbaladizo', 0, 'Sin dato'), ('Pavimento resbaladizo', 1, 'si'), ('Pavimento resbaladizo', 2, 'no'),
    ('Carga mal estibada', 0, 'Sin dato'), ('Carga mal estibada', 1, 'si'), ('Carga mal estibada', 2, 'no'),
    ('Condicion climática', 0, 'Sin dato'), ('Condicion climática', 1, 'si'), ('Condicion climática', 2, 'no'),
    ('No definida', 0, 'Sin dato'), ('No definida', 1, 'si'), ('No definida', 2, 'no')
]

# ==========================================================
# --- MAPAS DE DIMENSIONES (FICHA 1 - VEHÍCULOS) ---
# ==========================================================

map_service_type = {
    0: "Sin datos", 1: "Carabineros", 2: "Fiscal", 3: "Particular",
    4: "Trans.Escolar", 5: "Taxi Básico", 6: "Taxi Colectivo",
    7: "Bomberos", 8: "Ambulancia", 9: "L.Colectiva Urbana",
    10: "L. Colectiva Rural", 11: "L. Interprovincial",
    12: "L. Internacional", 13: "Carga Normal", 14: "Carga Peligrosa",
    99: "Otros"
}

map_maneuver_type = {
    0: "Sin datos", 1: "Viaja derecho por vía", 2: "Vira Derecha hacia Vía",
    3: "Vira Izquierda hacia Vía", 4: "Adelanta en Vía",
    5: "Detenido/deteniéndose en Vía", 6: "Retrocede en Vía",
    7: "Vira en U en Vía", 8: "Entra a Vía", 9: "Sale a Vía",
    10: "Estacionado en Calzada", 11: "Estacionado en Berma",
    12: "Cambia de pista en Vía", 13: "Reinicia marcha",
    14: "Cruzando la vía", 15: "Frena en vía", 99: "Otras"
}

map_consequence_type = {
    0: "Sin datos",
    1: "Con daños",
    2: "Sin daños"
}

# ==========================================================
# --- CONECTAR Y CARGAR TODAS LAS DIMENSIONES ---
# ==========================================================

try:
    conn = sqlite3.connect(ruta_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    try:
        print("\n--- Poblando Jerarquías de Vehículos y Tráfico ---")
        print("Poblando Nivel 1: dim_VehicleType...")
        cursor.executemany("INSERT OR IGNORE INTO dim_VehicleType (idVehicleType, VehicleType) VALUES (?, ?)", 
                           map_vehicle_type.items())
    except Exception as e:
        print(f"ERROR al poblar dim_VehicleType: {e}")
        raise # Detener el script si la jerarquía base falla

    try:
        print("Poblando Nivel 2: dim_Category...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Category (idCategory, CategoryName, idVehicleType) VALUES (?, ?, ?)", 
                           map_category)
    except Exception as e:
        print(f"ERROR al poblar dim_Category: {e}")
        raise # Detener el script si la jerarquía base falla
    
    try:
        print("Poblando Nivel 3: dim_VehicleTypeValue...")
        cursor.executemany("INSERT OR IGNORE INTO dim_VehicleTypeValue (idVehicleTypeValue, VehicleTypeName, idCategory) VALUES (?, ?, ?)", 
                           map_vehicle_type_value)
    except Exception as e:
        print(f"ERROR al poblar dim_VehicleTypeValue: {e}")
        raise # Detener el script si la jerarquía base falla

    print("\n--- Poblando Dimensiones de Tráfico ---")

    try:
        print("Poblando dim_Plaza...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Plaza (idPlaza, PlazaName) VALUES (?, ?)", map_plaza.items())
    except Exception as e:
        print(f"ERROR al poblar dim_Plaza: {e}")

    try:
        print("Poblando dim_Direction...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Direction (idDirection, DirectionName) VALUES (?, ?)", map_direction.items())
    except Exception as e:
        print(f"ERROR al poblar dim_Direction: {e}")

    print("\n--- Poblando Dimensiones de Accidentes (Ficha 0) ---")
    
    try:
        print("Poblando dim_AccidentType...")
        cursor.executemany("INSERT OR IGNORE INTO dim_AccidentType (idAccidentType, AccidentTypeName) VALUES (?, ?)", map_accident_type.items())
    except Exception as e:
        print(f"ERROR al poblar dim_AccidentType: {e}")

    try:
        print("Poblando dim_RelativeLocation...")
        cursor.executemany("INSERT OR IGNORE INTO dim_RelativeLocation (idRelativeLocation, RelativeLocationName) VALUES (?, ?)", map_relative_location.items())
    except Exception as e:
        print(f"ERROR al poblar dim_RelativeLocation: {e}")

    try:
        print("Poblando dim_SurfaceCondition...")
        cursor.executemany("INSERT OR IGNORE INTO dim_SurfaceCondition (idSurfaceCondition, SurfaceConditionName) VALUES (?, ?)", map_surface_condition.items())
    except Exception as e:
        print(f"ERROR al poblar dim_SurfaceCondition: {e}")
    
    try:
        print("Poblando dim_Weather...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Weather (idWeather, WeatherName) VALUES (?, ?)", map_weather.items())
    except Exception as e:
        print(f"ERROR al poblar dim_Weather: {e}")

    try:
        print("Poblando dim_Luminosity...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Luminosity (idLuminosity, LuminosityName) VALUES (?, ?)", map_luminosity.items())
    except Exception as e:
        print(f"ERROR al poblar dim_Luminosity: {e}")

    try:
        print("Poblando dim_ArtificialLight...")
        cursor.executemany("INSERT OR IGNORE INTO dim_ArtificialLight (idArtificialLight, ArtificialLightCondition) VALUES (?, ?)", map_artificial_light.items())
    except Exception as e:
        print(f"ERROR al poblar dim_ArtificialLight: {e}")

    try:
        print("Poblando dim_Lane...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Lane (LaneValue, LaneName) VALUES (?, ?)", map_lanes.items())
    except Exception as e:
        print(f"ERROR al poblar dim_Lane: {e}")

    try:
        print("Poblando dim_Environment...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Environment (EnvironmentCondition, EnvironmentValue, EnvironmentValueName) VALUES (?, ?, ?)", map_environment)
    except Exception as e:
        print(f"ERROR al poblar dim_Environment: {e}")

    try:
        print("Poblando dim_Response...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Response (ResponseType, ResponseValue, ResponseValueName) VALUES (?, ?, ?)", map_response)
    except Exception as e:
        print(f"ERROR al poblar dim_Response: {e}")

    try:
        print("Poblando dim_ProbableCause...")
        cursor.executemany("INSERT OR IGNORE INTO dim_ProbableCause (ProbableCauseType, CauseValue, CauseValueName) VALUES (?, ?, ?)", map_probable_cause)
    except Exception as e:
        print(f"ERROR al poblar dim_ProbableCause: {e}")

    try:
        print("Poblando dim_Consequence...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Consequence (ConsequenceType) VALUES (?)", [(c,) for c in map_consequence])
    except Exception as e:
        print(f"ERROR al poblar dim_Consequence: {e}")

    try:
        print("Poblando dim_Affected...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Affected (AffectedType) VALUES (?)", [(a,) for a in map_affected])
    except Exception as e:
        print(f"ERROR al poblar dim_Affected: {e}")

    try:
        print("Poblando dim_Section...")
        cursor.executemany("INSERT OR IGNORE INTO dim_Section (idSection, SectionName) VALUES (?, ?)", map_section.items())
    except Exception as e:
        print(f"ERROR al poblar dim_Section: {e}")

    print("\n--- Poblando Dimensiones de Vehículos (Ficha 1) ---")

    try:
        print("Poblando dim_ServiceType...")
        cursor.executemany("INSERT OR IGNORE INTO dim_ServiceType (idServiceType, ServiceName) VALUES (?, ?)", map_service_type.items())
    except Exception as e:
        print(f"ERROR al poblar dim_ServiceType: {e}")

    try:
        print("Poblando dim_ManeuverType...")
        cursor.executemany("INSERT OR IGNORE INTO dim_ManeuverType (idManeuverType, ManeuverType) VALUES (?, ?)", map_maneuver_type.items())
    except Exception as e:
        print(f"ERROR al poblar dim_ManeuverType: {e}")

    try:
        print("Poblando dim_ConsequenceType...")
        cursor.executemany("INSERT OR IGNORE INTO dim_ConsequenceType (idConsequenceType, ConsequenceType) VALUES (?, ?)", map_consequence_type.items())
    except Exception as e:
        print(f"ERROR al poblar dim_ConsequenceType: {e}")
        
    conn.commit()
    print("\nTodas las dimensiones han sido pobladas exitosamente.")

except Exception as e:
    print(f"Error al poblar dimensiones: {e}")
    conn.rollback()
finally:
    if conn:
        conn.close()