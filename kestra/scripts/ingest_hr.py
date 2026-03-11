import pandas as pd
import numpy as np
import fastparquet
import os, time
import logging
from sqlalchemy import create_engine
import great_expectations as gx
from datetime import datetime, timedelta
from pathlib import Path
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Configuration du Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sds.infra.pipeline")

# --- Initialisation des Connexions ---
# L'URL de la base de données est injectée via un secret Kestra dans les variables d'environnement
DATABASE_URL = os.getenv('DB_CONNECTION_STRING')
if not DATABASE_URL:
    logger.error("La chaîne de connexion à la base de données est manquante.")
    raise EnvironmentError("DB_CONNECTION_STRING non définie.")

engine = create_engine(DATABASE_URL)
MAX_ROWS = None


# Initialisation du géocodage
COMPANY_ADDR = "1362 Av. des Platanes, 34970 Lattes"

geolocator = Nominatim(user_agent="sport_pipeline_poc",timeout=5) # type: ignore

geocode_with_retry = RateLimiter(
    geolocator.geocode, 
    min_delay_seconds=1, 
    max_retries=3, 
    error_wait_seconds=2
)

location_comp = geolocator.geocode(COMPANY_ADDR)
coords_company = (location_comp.latitude, location_comp.longitude) # pyright: ignore[reportOptionalMemberAccess, reportAttributeAccessIssue]

hr_mapping = {
    'id' : np.int32 ,
    'last_name'  :object,
    'first_name' : object,
    'birthday' : None,
    'business_unit' : object,
    'entry_date' : None,
    'salary' : np.float64,
    'employement contract' :object,
    'vacation_days': np.int32,
    'address' : object,
    'transport_mode' : object,
}
sport_mapping = {
    'id' : np.int32,
    'practiced_sport' : object
}

hr_names = list(hr_mapping.keys())

TC = 'Transports en commun'
VM = 'véhicule thermique/électrique'
MR = 'Marche/running'
VT = 'Vélo/Trottinette/Autres' 

transport_limits = {
    TC : 0,
    VM : 0,
    MR : 15,
    VT : 25
}
transport_names = list(transport_limits.keys())
primed_transport = [MR,VT]

def validate_dataframe(df, suite_name, expectations_list):
    """
    Fonction générique de validation Great Expectations.
    Utilise le pattern WAP (Write-Audit-Publish) en mémoire.
    """
    context = gx.get_context()
    
    # Création d'une source de données Pandas éphémère pour GX
    datasource = context.data_sources.add_pandas(name=f"ds_{suite_name}")
    data_asset = datasource.add_dataframe_asset(name=f"asset_{suite_name}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"batch_{suite_name}")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    # Création et configuration de la suite d'attentes
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    
    for expectation in expectations_list:
        # Ajout dynamique des attentes selon la configuration fournie
        # Exemple : exp_type = gx.expectations.ExpectColumnValuesToNotBeNull
        suite.add_expectation(expectation)
            
    validation_result = batch.validate(suite)
    return validation_result

def get_distance(address):
    try:
        # Petite pause pour respecter les limites de l'API Nominatim
        time.sleep(1.5) 
        loc = geolocator.geocode(address)
        if loc:
            return round(geodesic(coords_company, (loc.latitude, loc.longitude)).km, 2) # type: ignore
        return None
    except Exception as e:
        print(f"Erreur pour l'adresse {address}: {e}")
        return None
    
def get_max_distance(transport_mode : str):
    """
    Get the max distance considering the transport mode
    """
    if transport_mode in transport_limits:
        return transport_limits[transport_mode]
    else:
        return 0


def run_load(hr_file_path, output_file):
    """
    HR xlsxs file loading.
    """

    logger.info(f"Début de l'ingestion des données RH : {hr_file_path}")
    try :
        df_rh = pd.read_excel(hr_file_path, names= hr_names, dtype= hr_mapping)
        df_rh.to_parquet(output_file)
    except Exception as e:
        print(f"Error during loading {hr_file_path}: {e}")
        return None


def run_etl(raw_file_parquet, output_file):
    """
    Ingestion du fichier RH avec validation pré-insertion.
    """
    logger.info(f"Start processing data  : {raw_file_parquet}")

    df_rh = pd.read_parquet(raw_file_parquet)
    if MAX_ROWS:
        df_rh = df_rh.head(MAX_ROWS)
    df_rh['distance_kms'] = 0
    df_rh['delta_distance'] = 0

    mask = df_rh['transport_mode'].isin(primed_transport)
    df_rh.loc[mask, 'distance_km'] = df_rh.loc[mask, 'address'].apply(get_distance)

    df_rh['delta_distance'] = df_rh['transport_mode'].apply(get_max_distance) - df_rh['distance_kms']
    df_rh.to_parquet(output_file)




def  run_check(processed_file_parquet):

    """
    Process file checking with GX.
    """
    logger.info(f"Start processing data  : {processed_file_parquet}")

    df_rh = pd.read_parquet(processed_file_parquet)
    min_date_birthday = datetime(1940, 1, 1)
    max_date_birthday = datetime(2010, 12, 31)
    min_date_entry = datetime(2020, 1, 1)
    max_date_entry = datetime.now()

    hr_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column= "id"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeUnique(column= "id"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(column= "transport_mode", value_set= transport_names), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'delta_distance', min_value= 0, max_value= None, strict_min=False), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'distance_kms', min_value= 0, max_value= 200, strict_min=False), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'birthday', min_value= min_date_birthday, max_value= max_date_birthday), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'entry_date', min_value= min_date_entry, max_value= max_date_entry), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(column_A="entry_date",   column_B="birthday"), # pyright: ignore[reportPrivateImportUsage]
    ]    


    results = validate_dataframe(df_rh, "hr_suite", hr_expectations)
    if results.success:
        # Pattern Publish : Insertion uniquement si la validation réussit
        logger.info("Données RH insérées avec succès dans PostgreSQL.")
    else:
        logger.error(f"Échec de la validation RH : {results}")
        # En production, on pourrait isoler les lignes erronées ici
        raise ValueError("Qualité des données RH insuffisante.")


def run_save(processed_file_parquet):
    """
    Process file checking with GX.
    """
    logger.info(f"Start saving data to postgres  : {processed_file_parquet}")

    df_rh = pd.read_parquet(processed_file_parquet)
    df_rh.drop(columns = 'delta_distance',inplace= True)
    df_rh.to_sql('employees', engine, if_exists='replace', index=False)





if __name__ == "__main__":
    # Ce bloc sera ignoré par Kestra en passant directement par les fonctions
    import sys
    # Usage: python ingest_hr.py <action> <source> <destination>
    action = sys.argv[1]
    MAX_ROWS = 5
    BASE_DIR = Path(__file__).parent.parent.parent

    if action == "load":
        run_load(f"{BASE_DIR}/data/sources/Données+RH.xlsx", f"{BASE_DIR}/kestra/tmp/rh_raw.parquet")
    elif action == "etl":
        run_etl(f"{BASE_DIR}/kestra/tmp/rh_raw.parquet", f"{BASE_DIR}/kestra/tmp/rh_processed.parquet")
    elif action == "check":
        run_check(f"{BASE_DIR}/kestra/tmp/rh_processed.parquet")
    elif action == "save":
        run_save("{BASE_DIR}/kestra/tmp/rh_processed.parquet")        