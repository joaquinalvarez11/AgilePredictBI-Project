import sqlite3
from config_manager import obtener_ruta

def run(callback):
    """
    Función principal para crear las tablas.
    Recibe un callback para enviar mensajes de log.
    """
    try:
        ruta_db = obtener_ruta("ruta_database")
        print(f"DEBUG: 'crear_tablas.py' intentará conectarse a: {ruta_db}")
    except Exception as e:
        print(f"Error al cargar ruta_database desde config_manager: {e}")
        raise

    conn = sqlite3.connect(ruta_db)
    cursor = conn.cursor()

    # Activar claves foráneas
    cursor.execute("PRAGMA foreign_keys = ON;")

    cursor.executescript("""

    /* ============================================= */
    /* --- 1. CREAR DIMENSIONES (SIN DEPENDENCIAS) --- */
    /* ============================================= */

    CREATE TABLE IF NOT EXISTS dim_DateTime (
        idDateTime INTEGER PRIMARY KEY,
        DateTime TEXT UNIQUE,
        Date TEXT,
        Year INTEGER,
        Month INTEGER,
        Day INTEGER,
        Hour INTEGER,
        Minute INTEGER,
        MonthName TEXT,
        WeekDay TEXT,
        WeekNumber INTEGER,
        Period TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_Section (
        idSection INTEGER PRIMARY KEY AUTOINCREMENT,
        Section INTEGER,
        SectionName TEXT
    ); 

    CREATE TABLE IF NOT EXISTS dim_Lane (
        idLane INTEGER PRIMARY KEY AUTOINCREMENT,
        LaneValue INTEGER UNIQUE,
        LaneName TEXT
    );
                
    /* --- Dimensiones Específicas de Accidentes (Ficha 0) --- */
    CREATE TABLE IF NOT EXISTS dim_AccidentType (
        idAccidentType INTEGER PRIMARY KEY,
        AccidentTypeName TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_SurfaceCondition (
        idSurfaceCondition INTEGER PRIMARY KEY,
        SurfaceConditionName TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_Weather (
        idWeather INTEGER PRIMARY KEY,
        WeatherName TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_Luminosity (
        idLuminosity INTEGER PRIMARY KEY,
        LuminosityName TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_ArtificialLight (
        idArtificialLight INTEGER PRIMARY KEY,
        ArtificialLightCondition TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_RelativeLocation (
        idRelativeLocation INTEGER PRIMARY KEY,
        RelativeLocationName TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_ProbableCause (
        idProbableCause INTEGER PRIMARY KEY AUTOINCREMENT,
        ProbableCauseType TEXT,
        CauseValue INTEGER,
        CauseValueName TEXT,
        UNIQUE(ProbableCauseType, CauseValue)
    );
    CREATE TABLE IF NOT EXISTS dim_Response (
        idResponse INTEGER PRIMARY KEY AUTOINCREMENT,
        ResponseType TEXT,
        ResponseValue INTEGER,
        ResponseValueName TEXT,
        UNIQUE(ResponseType, ResponseValue)
    );
    CREATE TABLE IF NOT EXISTS dim_Environment (
        idEnvironment INTEGER PRIMARY KEY AUTOINCREMENT,
        EnvironmentCondition TEXT,
        EnvironmentValue INTEGER,
        EnvironmentValueName TEXT,
        UNIQUE(EnvironmentCondition, EnvironmentValue)
    );
    CREATE TABLE IF NOT EXISTS dim_Consequence (
        idConsequence INTEGER PRIMARY KEY AUTOINCREMENT,
        ConsequenceType TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS dim_Affected (
        idAffected INTEGER PRIMARY KEY AUTOINCREMENT,
        AffectedType TEXT UNIQUE
    );

    /* --- Dimensiones Específicas de Vehículos (Ficha 1) --- */
    CREATE TABLE IF NOT EXISTS dim_ServiceType (
        idServiceType INTEGER PRIMARY KEY,
        ServiceName TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_ManeuverType (
        idManeuverType INTEGER PRIMARY KEY,
        ManeuverType TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_ConsequenceType (
        idConsequenceType INTEGER PRIMARY KEY,
        ConsequenceType TEXT
    );
    /* Esta es la versión correcta (sin FK) según tu última petición */
    CREATE TABLE IF NOT EXISTS dim_VehicleDescription (
        idVehicleDescription INTEGER PRIMARY KEY AUTOINCREMENT,
        Registration TEXT UNIQUE,
        Brand TEXT
    );

    /* --- Dimensiones Específicas de Tráfico --- */
    CREATE TABLE IF NOT EXISTS dim_Plaza (
        idPlaza INTEGER PRIMARY KEY,
        PlazaName TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS dim_Direction (
        idDirection INTEGER PRIMARY KEY,
        DirectionName TEXT UNIQUE
    );

    /* Nivel 1 de la Jerarquía de Vehículos (Padre) */
    CREATE TABLE IF NOT EXISTS dim_VehicleType (
        idVehicleType INTEGER PRIMARY KEY,
        VehicleType TEXT UNIQUE
    );
                

    /* ============================================== */
    /* --- 2. CREAR DIMENSIONES (CON DEPENDENCIAS) --- */
    /* ============================================== */
                
    /* Nivel 2 de la Jerarquía: dim_Category depende de dim_VehicleType */
    CREATE TABLE IF NOT EXISTS dim_Category (
        idCategory INTEGER PRIMARY KEY,
        CategoryName TEXT UNIQUE,
        idVehicleType INTEGER,
        FOREIGN KEY (idVehicleType) REFERENCES dim_VehicleType(idVehicleType)
    ); 

    /* Nivel 3 de la Jerarquía: dim_VehicleTypeValue depende de dim_Category */
    CREATE TABLE IF NOT EXISTS dim_VehicleTypeValue (
        idVehicleTypeValue INTEGER PRIMARY KEY,
        VehicleTypeName TEXT,
        idCategory INTEGER,
        FOREIGN KEY (idCategory) REFERENCES dim_Category(idCategory)
    );
                
    CREATE TABLE IF NOT EXISTS dim_Km (
        idKm INTEGER PRIMARY KEY,
        Km REAL UNIQUE,
        Element TEXT,
        Place TEXT,
        idPlaza INTEGER,
        FOREIGN KEY (idPlaza) REFERENCES dim_Plaza(idPlaza)
    ); 
                                    
    /* ================================= */
    /* --- 3. CREAR TABLAS DE HECHOS --- */
    /* ================================= */

    /* --- HECHO 1: ACCIDENTES --- */
    CREATE TABLE IF NOT EXISTS factAccident (
        idAccident TEXT PRIMARY KEY,
        idDateTime INTEGER,
        idSection INTEGER,
        idAccidentType INTEGER,
        idRelativeLocation INTEGER,
        idSurfaceCondition INTEGER,
        idWeather INTEGER,
        idLuminosity INTEGER,
        idArtificialLight INTEGER,
        InfrastructureDamage TEXT,
        Description TEXT,
        totalVehicles INTEGER,
        FOREIGN KEY (idDateTime) REFERENCES dim_DateTime(idDateTime),
        FOREIGN KEY (idSection) REFERENCES dim_Section(idSection),
        FOREIGN KEY (idAccidentType) REFERENCES dim_AccidentType(idAccidentType),
        FOREIGN KEY (idRelativeLocation) REFERENCES dim_RelativeLocation(idRelativeLocation),
        FOREIGN KEY (idSurfaceCondition) REFERENCES dim_SurfaceCondition(idSurfaceCondition),
        FOREIGN KEY (idWeather) REFERENCES dim_Weather(idWeather),
        FOREIGN KEY (idLuminosity) REFERENCES dim_Luminosity(idLuminosity),
        FOREIGN KEY (idArtificialLight) REFERENCES dim_ArtificialLight(idArtificialLight)
    );

    /* --- HECHO 2: VEHÍCULOS POR ACCIDENTE --- */
    CREATE TABLE IF NOT EXISTS factVehicleAccident (
        idVehicleAccident INTEGER PRIMARY KEY AUTOINCREMENT,
        idAccident TEXT,
        idVehicleDescription INTEGER,
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idVehicleDescription) REFERENCES dim_VehicleDescription(idVehicleDescription)
    );

    /* --- HECHO 3: TRÁFICO --- */
    CREATE TABLE IF NOT EXISTS factTraffic (
        idTraffic INTEGER PRIMARY KEY AUTOINCREMENT,
        idDateTime INTEGER,
        idPlaza INTEGER,
        idDirection INTEGER,
        idCategory INTEGER,
        trafficVolume INTEGER,
        FOREIGN KEY (idDateTime) REFERENCES dim_DateTime(idDateTime),
        FOREIGN KEY (idPlaza) REFERENCES dim_Plaza(idPlaza),
        FOREIGN KEY (idDirection) REFERENCES dim_Direction(idDirection),
        FOREIGN KEY (idCategory) REFERENCES dim_Category(idCategory)
    ); 


    /* ========================================================= */
    /* --- 4. CREAR TABLAS PUENTE Y DE DETALLE (DEPENDIENTES) --- */
    /* ========================================================= */

    /* --- Detalle de factAccident (M:N) --- */
    CREATE TABLE IF NOT EXISTS factAccidentAffected (
        idAccidentAffected INTEGER PRIMARY KEY AUTOINCREMENT,
        idAccident TEXT,
        idConsequence INTEGER,
        idAffected INTEGER,
        AffectedCount INTEGER,
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idConsequence) REFERENCES dim_Consequence(idConsequence),
        FOREIGN KEY (idAffected) REFERENCES dim_Affected(idAffected),
        UNIQUE(idAccident, idConsequence, idAffected)
    );

    /* --- Puentes de factAccident (M:N) --- */
    CREATE TABLE IF NOT EXISTS bridge_Accident_Lane (
        idAccident TEXT,
        idLane INTEGER,
        PRIMARY KEY (idAccident, idLane),
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idLane) REFERENCES dim_Lane(idLane)
    );
    CREATE TABLE IF NOT EXISTS bridge_Accident_ProbableCause (
        idAccident TEXT,
        idProbableCause INTEGER,
        PRIMARY KEY (idAccident, idProbableCause),
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idProbableCause) REFERENCES dim_ProbableCause(idProbableCause)
    );
    CREATE TABLE IF NOT EXISTS bridge_Accident_Response (
        idAccident TEXT,
        idResponse INTEGER,
        PRIMARY KEY (idAccident, idResponse),
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idResponse) REFERENCES dim_Response(idResponse)
    );
    CREATE TABLE IF NOT EXISTS bridge_Accident_Environment (
        idAccident TEXT,
        idEnvironment INTEGER,
        PRIMARY KEY (idAccident, idEnvironment),
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idEnvironment) REFERENCES dim_Environment(idEnvironment)
    );
    CREATE TABLE IF NOT EXISTS bridge_Accident_Km (
        idAccident TEXT,
        idKm INTEGER,
        PRIMARY KEY (idAccident, idKm),
        FOREIGN KEY (idAccident) REFERENCES factAccident(idAccident) ON DELETE CASCADE,
        FOREIGN KEY (idKm) REFERENCES dim_Km(idKm)
    );
                

    /* --- Puentes de factVehicleAccident --- */
    CREATE TABLE IF NOT EXISTS bridge_VehicleAccident_ServiceType (
        idVehicleAccident INTEGER,
        idServiceType INTEGER,
        PRIMARY KEY (idVehicleAccident, idServiceType),
        FOREIGN KEY (idVehicleAccident) REFERENCES factVehicleAccident(idVehicleAccident) ON DELETE CASCADE,
        FOREIGN KEY (idServiceType) REFERENCES dim_ServiceType(idServiceType)
    );
    CREATE TABLE IF NOT EXISTS bridge_VehicleAccident_VehicleTypeValue (
        idVehicleAccident INTEGER,
        idVehicleTypeValue INTEGER,
        PRIMARY KEY (idVehicleAccident, idVehicleTypeValue),
        FOREIGN KEY (idVehicleAccident) REFERENCES factVehicleAccident(idVehicleAccident) ON DELETE CASCADE,
        FOREIGN KEY (idVehicleTypeValue) REFERENCES dim_VehicleTypeValue(idVehicleTypeValue)
    );
    CREATE TABLE IF NOT EXISTS bridge_VehicleAccident_ManeuverType (
        idVehicleAccident INTEGER,
        idManeuverType INTEGER,
        PRIMARY KEY (idVehicleAccident, idManeuverType),
        FOREIGN KEY (idVehicleAccident) REFERENCES factVehicleAccident(idVehicleAccident) ON DELETE CASCADE,
        FOREIGN KEY (idManeuverType) REFERENCES dim_ManeuverType(idManeuverType)
    );
    CREATE TABLE IF NOT EXISTS bridge_VehicleAccident_ConsequenceType (
        idVehicleAccident INTEGER,
        idConsequenceType INTEGER,
        PRIMARY KEY (idVehicleAccident, idConsequenceType),
        FOREIGN KEY (idVehicleAccident) REFERENCES factVehicleAccident(idVehicleAccident) ON DELETE CASCADE,
        FOREIGN KEY (idConsequenceType) REFERENCES dim_ConsequenceType(idConsequenceType)
    );
    CREATE TABLE IF NOT EXISTS bridge_VehicleAccident_Lane (
        idVehicleAccident INTEGER,
        idLane INTEGER,
        PRIMARY KEY (idVehicleAccident, idLane),
        FOREIGN KEY (idVehicleAccident) REFERENCES factVehicleAccident(idVehicleAccident) ON DELETE CASCADE,
        FOREIGN KEY (idLane) REFERENCES dim_Lane(idLane)
    );


    /* ================================= */
    /* --- 5. CREAR TABLAS DE LOG ETL --- */
    /* ================================= */

    CREATE TABLE IF NOT EXISTS etl_log_ficha0 (
        FileName TEXT PRIMARY KEY,
        LoadedTimestamp TEXT
    );
    CREATE TABLE IF NOT EXISTS etl_log_ficha1_vehiculos (
        FileName TEXT PRIMARY KEY,
        LoadedTimestamp TEXT
    );
    CREATE TABLE IF NOT EXISTS etl_log_trafico (
        FileName TEXT PRIMARY KEY,
        LoadedTimestamp TEXT
    ); 

    """)

    conn.commit()
    conn.close()

    print("Tablas creadas correctamente.")
