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
from kestra import Kestra
            
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
KESTRA_MODE = True

FAILED = "Failed"
WARNING = "Warning"
SUCCESS = "Succes"

# Initialisation du géocodage
COMPANY_ADDR = "1362 Av. des Platanes, 34970 Lattes"

geolocator = Nominatim(user_agent="sport_pipeline_poc",timeout=5) # type: ignore

geocode_with_retry = RateLimiter(
    geolocator.geocode, 
    min_delay_seconds=1, 
    max_retries=3, 
    error_wait_seconds=2
)
GEO_DELAY = 1.5
COMPANY_LOCATION = geolocator.geocode(COMPANY_ADDR)
COMPANY_COORDS = (COMPANY_LOCATION.latitude, COMPANY_LOCATION.longitude) # pyright: ignore[reportOptionalMemberAccess, reportAttributeAccessIssue]

HR_MAPPING = {
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
SPORT_MAPPING = {
    'id' : np.int32,
    'sport_type' : object
}

HR_COLUMNS = list(HR_MAPPING.keys())
SPORT_COLUMNS = list(SPORT_MAPPING.keys())


TC = 'Transports en commun'
VM = 'véhicule thermique/électrique'
MR = 'Marche/running'
VT = 'Vélo/Trottinette/Autres' 

TRANSPORT_LIMIT = {
    TC : 0,
    VM : 0,
    MR : 15,
    VT : 25
}

PRIMED_TRANSPORT = [MR,VT]
# Global for scope
SPORTS_LIST = []
SPORT_REPLACE = {
    "Runing" : "Course à pied",

}

############################################
# COMMON FUNCTIONS
############################################

def normalize_str(text:str) -> str:
    location = text.strip().lower()
    """ Remove accents from text """
    accents = { 'a': ['à', 'ã', 'á', 'â'],
                'e': ['é', 'è', 'ê', 'ë'],
                'i': ['î', 'ï'],
                'u': ['ù', 'ü', 'û'],
                'o': ['ô', 'ö'],
                ' ': ['-','/'] 
                }
    for (char, accented_chars) in accents.items():
        for accented_char in accented_chars:
            location = location.replace(accented_char, char)
    return location    


def kestra_output(name : str, value):
    """
    Allow kestra output vars when running in kestra
    or simple print in CLI mode
    """

    if KESTRA_MODE:
        Kestra.outputs({name: value})
    else:
        print(f" Kestra Ouput -> {name}: {str(value)}")        

def extract_xlsx(filtetype:str , file_path, output_file):
    """
    HR xlsxs file loading.
    """

    logger.info(f"Start extracting data from Excel : {file_path}")
    try :
        if filtetype == "hr":
            df = pd.read_excel(file_path, names= HR_COLUMNS, dtype= HR_MAPPING)
        else:
            df = pd.read_excel(file_path, names= SPORT_COLUMNS, dtype= SPORT_MAPPING)

        df.to_parquet(output_file)
        kestra_output("status", SUCCESS)
        kestra_output("dims", df.shape)
    except Exception as e:
        logger.error(f"Error during loading {file_path}: {e}")
        raise FileNotFoundError(f"File {file_path} not found (Critical error).")

    logger.info(f"End extracting HR data to  : {output_file}")



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
    
    # Create context
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    
    for expectation in expectations_list:
        # Ajout dynamique des attentes selon la configuration fournie
        # Exemple : exp_type = gx.expectations.ExpectColumnValuesToNotBeNull
        suite.add_expectation(expectation)
            
    validation_result = batch.validate(suite)
    
    # On calcule si le flow doit REELLEMENT s'arrêter
    # On considère un échec VRAI seulement si l'expectation n'est pas marquée 'warning'
    is_critically_failed = False

    k_ouput = ""
    status = SUCCESS
    
    for result in validation_result.results:
        if result.success:
           k_ouput += f"✅ Expectation : {result.expectation_config.get("description", "")} : Success\n"  # type: ignore
        else:
            flaws = result.result.get('partial_unexpected_list',[])
            flaws_txt = result.result.get('artial_unexpected_list')
            severity = result.expectation_config.meta.get("severity", "critical")  # type: ignore

            if severity == "critical":
                k_ouput += f"❌ Expectation : {result.expectation_config.get("description")} : Fail\n"  # type: ignore
                status = FAILED
            else:
                k_ouput += f"⚠️ Expectation : {result.expectation_config.get("description")} : Warning\n"  # type: ignore
                if status ==  SUCCESS: status = WARNING
            k_ouput += "- " + f'{"\n- ".join(flaws)}' + "\n"

    kestra_output('detail', k_ouput)
    kestra_output('status', status)

    if status == SUCCESS:
        logger.info(f"Succeed to validate {suite_name} data with gX.")
    elif status == WARNING:
        # SPECIFIC CASE : no errors except  WARNINGS
        logger.warning(f"Warning during {suite_name} validation with gX")
    else:
        # CRITICAL CASE : Au moins une erreur 'critical' (comme l'ID)
        logger.error(f"Fail to validate {suite_name} with gX (Critical). : ")
        raise ValueError(f"{suite_name} quality not sufficient (Critical error).")

############################################
# HR EMPLOYEES FUNCTIONS
############################################

def get_distance(address):
    try:
        # Need a delay to respect API Nominatim rules
        time.sleep(GEO_DELAY) 
        loc = geolocator.geocode(address)
        if loc:
            return round(geodesic(COMPANY_COORDS, (loc.latitude, loc.longitude)).km, 2) # type: ignore
        return None
    except Exception as e:
        print(f"Erreur pour l'adresse {address}: {e}")
        return None
    
def get_max_distance(transport_mode : str):
    """
    Get the max distance considering the transport mode
    """
    if transport_mode in TRANSPORT_LIMIT:
        return TRANSPORT_LIMIT[transport_mode]
    else:
        return 0

def transform_hr(raw_file_parquet, output_file):
    """
    Ingestion du fichier RH avec validation pré-insertion.
    """
    logger.info(f"Start transforming HR raw data from : {raw_file_parquet}")

    df_rh = pd.read_parquet(raw_file_parquet)
    if MAX_ROWS:
        df_rh = df_rh.head(MAX_ROWS)

    df_rh['distance_kms'] = 0.0
    mask = df_rh['transport_mode'].isin(PRIMED_TRANSPORT)
    relevants = df_rh[mask].shape[0]
    kestra_output("relevants", relevants)

    logger.info(f"Start gathering location for {relevants} relevant employees")
    logger.info(f"Could be long, waiting {GEO_DELAY} second(s) between employees")

    df_rh.loc[mask, 'distance_kms'] = df_rh.loc[mask, 'address'].apply(get_distance)

    df_rh['margin_kms'] = df_rh['transport_mode'].apply(get_max_distance) - df_rh['distance_kms']

    df_rh.to_parquet(output_file)
    kestra_output("status", SUCCESS)
    logger.info(f"End transforming HR raw data into {output_file}")



def  validate_hr(processed_file_parquet):

    """
    Process hr employees file checking with GX.
    """
    logger.info(f"Start validating HR data  : {processed_file_parquet}")

    df_rh = pd.read_parquet(processed_file_parquet)

    transport_names = list(TRANSPORT_LIMIT.keys())
    min_date_birthday = datetime(1940, 1, 1)
    max_date_birthday = datetime(2010, 12, 31)
    min_date_entry = datetime(2020, 1, 1)
    max_date_entry = datetime.now()

    hr_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column= "id", description= "No missing ids"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeUnique(column= "id", description= "No duplicated ids"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(column= "transport_mode", value_set= transport_names, description= "Transport mode is in the list"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'margin_kms', min_value= 0, max_value= None, strict_min=False, description= "Home distance and transport mode are consistent"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'distance_kms', min_value= 0, max_value= 200, strict_min=False, description= "Home distance are lower than 200kms"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'birthday', min_value= min_date_birthday, max_value= max_date_birthday, description= "Birthdays are > 1940/1/1 and < 2012/12/21"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeBetween(column = 'entry_date', min_value= min_date_entry, max_value= max_date_entry, description= "Entry dates are > 2020/1/1/"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(column_A="entry_date",   column_B="birthday", description= "Entry date later than birthday"), # pyright: ignore[reportPrivateImportUsage]
    ]    
    validate_dataframe(df_rh, "HR_Data", hr_expectations)
    logger.info(f"End validating HR data  : {processed_file_parquet}")


############################################
# HR SPORT FUNCTIONS
############################################

def get_normalized_sport(sport_name):
    
    if not sport_name:  
        return None
    if sport_name in SPORT_REPLACE.keys():
        return SPORT_REPLACE[sport_name]
    
    for sport in SPORTS_LIST:
        if normalize_str(sport_name) == normalize_str(sport):
            return sport
    return sport_name


def load_sport_file(json_sport_file):

    logger.info(f"Start loading Strava Sports List  : {json_sport_file}")

    try:
        global SPORTS_LIST
        sports_df = pd.read_json(json_sport_file)
        SPORTS_LIST = list(sports_df['sport'])
    except:
        logger.error(f"Fail to load strava sports list : {json_sport_file}")
        # En production, on pourrait isoler les lignes erronées ici
        raise FileNotFoundError(f"Fail to load strava sports list : {json_sport_file}")

 

def transform_sport(raw_file_parquet, output_file,json_sport_file):
    """
    Sport data File transformation
    """
    logger.info(f"Start transforming sport raw data from : {raw_file_parquet}")
    load_sport_file(json_sport_file)
    df_sport = pd.read_parquet(raw_file_parquet)
    df_sport['sport_type'] = df_sport['sport_type'].apply(get_normalized_sport)

    df_sport.to_parquet(output_file)
    kestra_output("status", SUCCESS)
    logger.info(f"End transforming sport raw data into {output_file}")


def  validate_sport(processed_file_parquet,json_sport_file):

    """
    Process sport file checking with GX.
    """
    logger.info(f"Start processing data  : {processed_file_parquet}")

    load_sport_file(json_sport_file)
    df_sport = pd.read_parquet(processed_file_parquet)

    hr_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column= "id", description= "No missing ids"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeUnique(column= "id", description= "No duplicated ids"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(column= "sport_type", value_set= SPORTS_LIST,meta={ "severity": "warning", "notes": "Sports not present in Strava System" }, description= "Sport name in Strava refs"), # pyright: ignore[reportPrivateImportUsage]
    ]    

    validate_dataframe(df_sport, "Sport_Data", hr_expectations)
    logger.info(f"End validating Sport data  : {processed_file_parquet}")


############################################
# HR EMPLOYEES + SPORT FUNCTIONS
############################################

def merge_hr_sport(hr_file_path, sport_file_path, output_file):
    logger.info(f"Start processing data  : {hr_file_path} + {sport_file_path}")

    df_rh = pd.read_parquet(hr_file_path)
    df_sport = pd.read_parquet(sport_file_path)
    df_final = df_rh.merge(df_sport,how='left',left_on="id", right_on="id")
    
    df_rh.to_parquet(output_file)
    kestra_output("status", SUCCESS)
    logger.info(f"End transforming HR raw data into {output_file}")


def  validate_merge(merge_file_parquet,json_sport_file):

    """
    Process merged file checking with GX.
    """
    logger.info(f"Start processing data  : {merge_file_parquet}")

    load_sport_file(json_sport_file)
    df_sport = pd.read_parquet(merge_file_parquet)

    hr_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column= "id", description= "No missing ids"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnValuesToBeUnique(column= "id", description= "No duplicated ids"), # pyright: ignore[reportPrivateImportUsage]
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(column= "sport_type", value_set= SPORTS_LIST,meta={ "severity": "warning", "notes": "Sports not present in Strava System" }, description= "Sport name in Strava refs"), # pyright: ignore[reportPrivateImportUsage]
    ]    

    validate_dataframe(df_sport, "Sport_Data", hr_expectations)
    logger.info(f"End validating Sport data  : {merge_file_parquet}")


def load_pg(merge_file_parquet):
    """
    Process file checking with GX.
    """
    logger.info(f"Start saving data to postgres  : {merge_file_parquet}")

    df_rh = pd.read_parquet(merge_file_parquet)
    df_rh.drop(columns = 'margin_kms',inplace= True)
    df_rh.to_sql('employees', engine, if_exists='replace', index=False)

############################################
# CLI ENDPOINT
############################################


if __name__ == "__main__":
    # KESTRA must avoid this, calling functions directly
    # Allow CLI debugging
    import sys
    # Usage: python ingest_hr.py <action> <source> <destination>
    action = sys.argv[1]
    MAX_ROWS = 5
    BASE_DIR = Path(__file__).parent.parent.parent
    KESTRA_MODE = False

    if action == "extract-rh":
        extract_xlsx('hr',f"{BASE_DIR}/data/sources/Données+RH.xlsx", f"{BASE_DIR}/kestra/tmp/rh_raw.parquet")
    elif action == "transform-rh":
        transform_hr(f"{BASE_DIR}/kestra/tmp/rh_raw.parquet", f"{BASE_DIR}/kestra/tmp/rh_processed.parquet")
    elif action == "validate-hr":
        validate_hr(f"{BASE_DIR}/kestra/tmp/rh_processed.parquet")
    if action == "extract-sport":
        extract_xlsx('sport',f"{BASE_DIR}/data/sources/Données+Sportive.xlsx", f"{BASE_DIR}/kestra/tmp/sport_raw.parquet")
    elif action == "transform-sport":
        transform_sport(f"{BASE_DIR}/kestra/tmp/sport_raw.parquet", f"{BASE_DIR}/kestra/tmp/sport_processed.parquet", f"{BASE_DIR}/data/sources/strava_sports.json")
    elif action == "validate-sport":
        validate_sport(f"{BASE_DIR}/kestra/tmp/sport_processed.parquet", f"{BASE_DIR}/data/sources/strava_sports.json")
    elif action == "merge":
        merge_hr_sport(f"{BASE_DIR}/kestra/tmp/rh_processed.parquet",f"{BASE_DIR}/kestra/tmp/sport_processed.parquet",f"{BASE_DIR}/kestra/tmp/merge.parquet")        
    elif action == "load-pg":
        load_pg(f"{BASE_DIR}/kestra/tmp/rh_processed.parquet")                