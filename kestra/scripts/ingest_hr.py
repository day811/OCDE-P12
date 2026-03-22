import pandas as pd
import numpy as np
import fastparquet
import os, time, sys, warnings, io
from typing import List, Any, Tuple, Optional
from enum import Enum
import logging
from sqlalchemy import create_engine, text, engine as Pse
import great_expectations as gx
from datetime import datetime
from pathlib import Path
from geopy.distance import geodesic
from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter
import common_tools as ct

# On ne met PAS de stream=sys.stdout ici
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("sds.infra.ingest_hr")
logger.propagate = False # Évite de doubler les logs avec la config de base
logger.setLevel(logging.INFO)

# Handler 1 : Les INFOS vont vers STDOUT (Vert dans Kestra)
h_info = logging.StreamHandler(sys.stdout)
h_info.setLevel(logging.INFO)
h_info.addFilter(lambda record: record.levelno <= logging.INFO) # Stricte INFO

# Handler 2 : Les WARNINGS/ERRORS vont vers STDERR (Orange/Rouge dans Kestra)
h_warn = logging.StreamHandler(sys.stderr)
h_warn.setLevel(logging.WARNING) # Capte WARNING, ERROR, CRITICAL

# Formatage
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
h_info.setFormatter(formatter)
h_warn.setFormatter(formatter)

logger.addHandler(h_info)
logger.addHandler(h_warn)

# --- Initialisation des Connexions ---
# L'URL de la base de données est injectée via un secret Kestra dans les variables d'environnement

                      
DATABASE_URL = os.getenv('DB_CONNECTION_STRING',"")
if not DATABASE_URL:
    logger.error("La chaîne de connexion à la base de données est manquante.")
    raise EnvironmentError("DB_CONNECTION_STRING non définie.")


# GEOCODING INITIALIZATION

GOOGLE_MAPS_KEY = os.getenv('GOOGLE_MAPS_KEY',"")
COMPANY_ADDR = "1362 Av. des Platanes, 34970 Lattes"

geolocator = GoogleV3(api_key=GOOGLE_MAPS_KEY, timeout=10)  # type: ignore

geocode_with_retry = RateLimiter(
    geolocator.geocode, 
    min_delay_seconds=0.2, 
    max_retries=3, 
    error_wait_seconds=4
)

# GEOLOC : ALL : Fetch all coordonates
# GEOLOC : RELEVANTS : Fetch coordonates for prime candidates only
# GEOLOC : NONE : Fetch no coordonates, for testing without requiring API
class GlocMode(Enum):
    All = "All"
    Relevants = "Relevants"
    Nobody = 'None'

GEO_LOC_MODE:GlocMode = GlocMode.Nobody 
# Delay time between each api request 
GEO_DELAY = 0.15
COMPANY_COORDS = (0.0, 0.0)

if GEO_LOC_MODE != GlocMode.Nobody:
    try:
        COMPANY_LOCATION = geolocator.geocode(COMPANY_ADDR)
        if COMPANY_LOCATION:
            COMPANY_COORDS = (COMPANY_LOCATION.latitude, COMPANY_LOCATION.longitude) # type: ignore
        else:
            logger.warning("Coordonnées de l'entreprise introuvables. Mode dégradé (distance = 0).")
            raise ValueError(f"Company coordinates not found (Critical error).")
    except Exception as e:
        logger.error(f"Erreur lors du géocodage de l'entreprise : {e}")


#FIELDS REQUIREMENTS
class Ttype(Enum):
    @classmethod
    def values(cls):
        return [member.value for member in cls]
    TC = 'Transports en commun'
    VM = 'véhicule thermique/électrique'
    MR = 'Marche/running'
    VT = 'Vélo/Trottinette/Autres' 

TRANSPORT_LIMIT = {
    Ttype.TC.value : 150,
    Ttype.VM.value : 250,
    Ttype.MR.value : 15,
    Ttype.VT.value : 25
}
ALL_TRANSPORTS = Ttype.values()
PRIMED_TRANSPORT = [Ttype.MR.value,Ttype.VT.value]

CONTRACT_TYPES = ['CDI','CDD']


############################################
# COMMON FUNCTIONS
############################################



def validate_dataframe(df: pd.DataFrame, suite_name: str, expectations_list: List[Any]) -> Tuple[str, List[str]]:
    """
    Generic Great Expectations validation function using the WAP (Write-Audit-Publish) pattern.

    Args:
        df: The DataFrame to validate.
        suite_name: Name of the expectation suite.
        expectations_list: List of GX expectation objects to run.

    Returns:
        A tuple containing the global status (ct.SUCCESS/ct.WARNING/ct.CRITICAL) and detailed logs.
    """

    context = gx.get_context()
    
    # Création d'une source de données Pandas éphémère pour GX
    datasource = context.data_sources.add_pandas(name=f"ds_{suite_name}")
    data_asset = datasource.add_dataframe_asset(name=f"asset_{suite_name}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"batch_{suite_name}")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    # Create context
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    stderr_backup = sys.stderr # On sauvegarde le vrai flux d'erreur
    sys.stderr = io.StringIO()  # On redirige stderr vers un buffer vide en mémoire
    
    for expectation in expectations_list:
        # Ajout dynamique des attentes selon la configuration fournie
        # Exemple : exp_type = gx.expectations.ExpectColumnValuesToNotBeNull
        suite.add_expectation(expectation)
            
    try:
        # L'appel qui génère la pollution
        validation_result = batch.validate(suite)
    finally:
        sys.stderr = stderr_backup # On restaure le vrai flux d'erreur quoi qu'il arrive
    
    
    
    status = ct.SUCCESS
    details = []
    for result in validation_result.results:
        severity = result.expectation_config.meta.get(ct.SEVERITY, ct.FAILED)  # type: ignore
        keep_row = result.expectation_config.meta.get(ct.KEEP_ROW, True) # type: ignore
        if result.success:
           details.append(f"Expectation : {result.expectation_config.get('description', '')} ({severity}): {ct.SUCCESS}")  # type: ignore
        else:
            flaws = result.result.get('partial_unexpected_list',[])
            flaws_index = result.result.get('partial_unexpected_index_list',[])

            if severity == ct.FAILED:
                status = ct.FAILED
                details.append(f"Expectation : {result.expectation_config.get('description',)}  ({severity}): {ct.FAILED}")  # type: ignore
            else:
                details.append(f"Expectation : {result.expectation_config.get('description',)}  ({severity}): {ct.WARNING}")  # type: ignore
                if status ==  ct.SUCCESS: status = ct.WARNING
            if len(flaws): 
                if len(flaws_index):
                    flaw_output = f"{ct.SP4}Invalid values list :"
                    for index_flaw, flaw in zip(flaws_index,flaws):
                        reference = str(df.at[index_flaw, "id"])
                        if not reference or reference == "nan":
                            reference = f"{str(df.loc[index_flaw, 'first_name'])} {str(df.loc[index_flaw, 'last_name'])}"
                        flaw_output +=  f"\n{ct.SP4}- Employee : {reference} --> Ligne xlsx -> {str(index_flaw)}"       
                else:
                    flaw_output = f"- Invalid values list :\n{ct.SP4}- "
                    flaw_output +=  f"\n{ct.SP4}- ".join([str(flaw) for flaw in flaws])

                details.append(f'{flaw_output}')

    if status == ct.SUCCESS:
        logger.info(f"Succeed to validate {suite_name} data with gX.")
    elif status == ct.WARNING:
        # SPECIFIC CASE : no errors except  ct.WARNINGS
        logger.warning(f"Warning during {suite_name} validation with gX")
    else:
        # CRITICAL CASE : Au moins une erreur 'critical' (comme l'ID)
        logger.error(f"Fail to validate {suite_name} with gX (Critical). : ")
    return status, details

############################################
# HR EMPLOYEES FUNCTIONS
############################################

def get_coordinates(address: Optional[str]) :
    """
    Calculates the coordinates of a given address using Google Geocoding.

    Args:
        address: The physical address of the employee.

    Returns:
        Distance in kilometers (float) or None if geocoding fails.
    """

    try:
        if GEO_LOC_MODE:
            # Need a delay to respect API Nominatim rules
            time.sleep(GEO_DELAY) 
            loc = geolocator.geocode(address)
            if loc:
                return (loc.latitude, loc.longitude) # type: ignore
            return None
        else :
            return None
    except Exception as e:
        logger.error(f"Erreur pour l'adresse {address}: {e}")
        return None
    
def get_distance(employees_coord) -> Optional[float]:
    """
    Calculates the geodesic distance between the company and a given address using Google Geocoding.

    Args:
        address: The physical address of the employee.

    Returns:
        Distance in kilometers (float) or None if geocoding fails.
    """

    if GEO_LOC_MODE:
        if employees_coord :
            return round(geodesic(COMPANY_COORDS, employees_coord).km, 2) # type: ignore
        else:
            return None
    return 0 # set distance to 0 when GEO_LOC_MODE is not activated

    
def get_max_distance(transport_mode: str) -> int:
    """
    Retrieves the maximum allowed distance for a specific transport mode.

    Args:
        transport_mode: The mode of transport used by the employee.

    Returns:
        The distance limit in km.
    """

    if transport_mode in TRANSPORT_LIMIT:
        return TRANSPORT_LIMIT[transport_mode]
    else:
        return 0

def load_hr_xlsx(file_path: str, output_file: str):

    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try :
        df = ct.extract_xlsx(file_path, names= ct.HR_COLUMNS, mapping= ct.HR_MAPPING,header=0)
        details=[]
        df['address'] = df['address'].apply(ct.encrypt_text)    
        details.append("Crypt all addresses")

        df['first_name'] = df['first_name'].apply(ct.encrypt_text)    
        details.append("Crypt all first_names")

        df['last_name'] = df['last_name'].apply(ct.encrypt_text)    
        details.append("Crypt all last_names")

        shape = df.shape
        df.to_parquet(output_file)

    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))


def transform_hr(raw_file_parquet: str, output_file: str) -> None:
    """
    Transforms raw HR data by decrypting addresses, calculating age, 
    seniority, and commuting distances.

    Args:
        raw_file_parquet: Path to the raw Parquet file.
        output_file: Path to save the processed Parquet file.
    """
    shape = (0,0)
    status = ct.SUCCESS
    details = []
    try :
        logger.info(f"Start transforming HR raw data from : {raw_file_parquet}")
        details = []

        df_rh = pd.read_parquet(raw_file_parquet)
        if ct.MAX_ROWS:
            df_rh = df_rh.head(ct.MAX_ROWS)
        df_rh['address'] = df_rh['address'].apply(ct.decrypt_text)    
        details.append("Decrypt all addresses")

        ct.kestra_output("geoloc", str(GEO_LOC_MODE).capitalize())
        df_rh['distance_kms'] = 0.0
        if GEO_LOC_MODE == GlocMode.Nobody:
            relevants = 0
        else:
            if GEO_LOC_MODE == GlocMode.Relevants:
                mask: pd.Series = df_rh['transport_mode'].isin(PRIMED_TRANSPORT)
            elif GEO_LOC_MODE == GlocMode.All:
                mask: pd.Series = df_rh['transport_mode'].isin(ALL_TRANSPORTS)

            relevants = len(df_rh[mask])
            logger.info(f"Start gathering location coordinates for {relevants} relevant employees")
            logger.info(f"Could be long, waiting {GEO_DELAY} second(s) between employees")

            df_rh.loc[mask, 'distance_kms'] = df_rh.loc[mask, 'address'].apply(get_distance)

        details.append(f"Calculating distance from company for {relevants} employees ")

        birth_dates = pd.to_datetime(df_rh['birthday'], errors='coerce')
        df_rh['age'] = (datetime.now() - birth_dates).dt.days//365
        df_rh['age'] = df_rh['age'].fillna(0).astype(int)
        details.append(f"Calculating employee ages")
        
        entry_dates = pd.to_datetime(df_rh['entry_date'], errors='coerce')
        df_rh['seniority_years'] = (datetime.now() - entry_dates).dt.days // 365
        df_rh['seniority_years'] = df_rh['seniority_years'].fillna(0).astype(int)
        details.append(f"Calculating seniority years for employees")

        df_rh['margin_kms'] = df_rh['transport_mode'].apply(get_max_distance) - df_rh['distance_kms']
        details.append(f"Calculating difference betweem transport max distance and employees home distance")

        cols_to_drop = ['birthday', 'entry_date','address']
        df_rh.drop(columns = cols_to_drop, inplace= True)
        details.append(f"Remove cols : {' - '.join(cols_to_drop)}")

        shape = df_rh.shape
        df_rh.to_parquet(output_file)

        logger.info(f"End transforming HR raw data into {output_file}")    
    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))

    

def get_hr_expectations():

    min_age = 16
    max_age = 80
    min_seniority_years = 0
    max_seniority_years = 7
    min_salary = 15000
    max_salary = 150000

    distance_rule = f"{ct.SP2}\n{ct.SP4}- " + f"{ct.SP2}\n{ct.SP4}- ".join([f"{index} <= {TRANSPORT_LIMIT[index]}kms" for index in TRANSPORT_LIMIT.keys()]) + f"{ct.SP2}\n"
    contract_rule = f"[{','.join(CONTRACT_TYPES)}]"
    transport_rule = f"\n[{' , '.join(ALL_TRANSPORTS)}]\n"

    hr_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(  # type: ignore
            column= "id", 
            description= "No missing ids",
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : False   },
            ), 
        gx.expectations.ExpectColumnValuesToBeUnique( # type: ignore
            column= "id", 
            description= "No duplicated ids",
            meta={ ct.SEVERITY: ct.FAILED }, 
            ), 
        gx.expectations.ExpectColumnValuesToNotBeNull( # type: ignore
            column= "first_name", 
            description= "No missing first names",
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : True  },
            ),
        gx.expectations.ExpectColumnValuesToNotBeNull( # type: ignore
            column= "last_name", 
            description= "No missing last_names",
            meta={ ct.SEVERITY: ct.FAILED }, 
            ),
        gx.expectations.ExpectColumnValuesToBeInSet( # type: ignore
            column= "transport_mode", 
            value_set= ALL_TRANSPORTS, 
            description= f"Transport mode is in the list :  {transport_rule}",
            meta={ ct.SEVERITY: ct.FAILED }, 
            ),
        gx.expectations.ExpectColumnValuesToNotBeNull( # type: ignore
            column= "distance_kms", 
            description= "No missing required distances",
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : False   },
            ),
        gx.expectations.ExpectColumnValuesToBeInSet( # type: ignore
            column= "employement_contract", 
            value_set= CONTRACT_TYPES, description= f"Employement contract is in {contract_rule}",
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : True   },
            ),
        gx.expectations.ExpectColumnValuesToBeBetween( # type: ignore
            column = 'margin_kms',
            description= f"Transport mode and home distance are consistent {distance_rule}", 
            min_value= 0, max_value= None, 
            strict_min=False, 
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : False   },
            ),
        gx.expectations.ExpectColumnValuesToBeBetween( # type: ignore
            column = 'age', 
            description= f"Employees age between {min_age} and {max_age}", 
            min_value= min_age, max_value= max_age,
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : True   },
            ), 
        gx.expectations.ExpectColumnValuesToBeBetween( # type: ignore
            column = 'seniority_years', min_value= min_seniority_years, max_value= max_seniority_years, 
            description= f"Employees senority years between {min_seniority_years} and {max_seniority_years}",
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : True   },
            ), 
        gx.expectations.ExpectColumnValuesToBeBetween( # type: ignore
            column = 'salary',
            description= f"Employee salaries between {min_salary} and {max_salary}", 
            min_value= min_salary, max_value= max_salary ,
            meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : False   },
            ), 
        ]    
    return hr_expectations


def validate_hr(processed_file_parquet: str) -> None:
    """
    Validates processed HR data against business rules using Great Expectations.

    Args:
        processed_file_parquet: Path to the Parquet file to validate.

    Raises:
        ValueError: If critical data quality expectations are not met.
    """
    details = []
    status = ct.SUCCESS
    try :
        logger.info(f"Start validating HR data  : {processed_file_parquet}")

        df_rh = pd.read_parquet(processed_file_parquet)
        details=[]
        df_rh['first_name'] = df_rh['first_name'].apply(ct.decrypt_text)    
        details.append("Decrypt all first names for validation")

        df_rh['last_name'] = df_rh['last_name'].apply(ct.decrypt_text)    
        details.append("Decrypt all last names for validation")

        hr_expectations = get_hr_expectations()
        status, new_details = validate_dataframe(df_rh, "HR_Data", hr_expectations)
        details += new_details
        logger.info(f"End validating HR data  : {processed_file_parquet}")
    except Exception as e:
        logging.error( f"Critical Error : {e}")
        status = ct.FAILED


    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))
    




############################################
# HR SPORT FUNCTIONS
############################################


def load_sport_xlsx(file_path: str, output_file: str):
    
    shape = (0,0)
    status = ct.SUCCESS
    try :
        df = ct.extract_xlsx(file_path, names= ct.SPORT_COLUMNS, header=0)
        shape = df.shape
        df.to_parquet(output_file)

    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))



def transform_sport(raw_file_parquet: str, output_file: str, excel_sport_file: str) -> None:
    """
    Normalizes sport names in the sportive data using the Sport_engine aliases.

    Args:
        raw_file_parquet: Path to the raw sportive Parquet file.
        output_file: Path for the normalized Parquet file.
        excel_sport_file: Path to the sports reference Excel file.
    """

    shape = (0,0)
    status = ct.SUCCESS
    try :
        logger.info(f"Start transforming sport raw data from : {raw_file_parquet}")

        details = []
        sport_engine = ct.Sport_engine(excel_sport_file, ct.START_DATE)
        df_sport = pd.read_parquet(raw_file_parquet)
        #df_sport['strava_sport'] = df_sport['sport_type'].apply(sport_engine.get_normalized_sport)
        #details.append(f"Find sport in strava list and aliases")

        shape =  df_sport.shape
        df_sport.to_parquet(output_file)

        ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
        logger.info(f"End transforming sport raw data into {output_file}")
    except Exception as e:
        status = ct.FAILED
        f"Error: {e}"

    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))

 



def validate_sport(processed_file_parquet: str, excel_sport_file: str) -> None:
    """
    Checks that all declared sports in the file exist in the Strava reference list.

    Args:
        processed_file_parquet: Path to the processed sportive Parquet file.
        excel_sport_file: Path to the sports reference Excel file.

    Raises:
        ValueError: If critical mismatches are found.
    """

    status = ct.SUCCESS
    details=[]
    try :
        logger.info(f"Start processing data  : {processed_file_parquet}")

        sport_engine = ct.Sport_engine(excel_sport_file, ct.START_DATE)

        df_sport = pd.read_parquet(processed_file_parquet)

        hr_expectations = [
            gx.expectations.ExpectColumnValuesToNotBeNull( # pyright: ignore[reportPrivateImportUsage]
                column= "id", 
                description= "No missing ids", 
                meta={ ct.SEVERITY: ct.WARNING , ct.KEEP_ROW : False },
                ) ,
            gx.expectations.ExpectColumnValuesToBeUnique( # pyright: ignore[reportPrivateImportUsage]
                column= "id",
                description= "No duplicated ids", 
                meta={ ct.SEVERITY: ct.FAILED }, 
                ),
            gx.expectations.ExpectColumnDistinctValuesToBeInSet( # pyright: ignore[reportPrivateImportUsage]
                column= "sport_type", 
                value_set= sport_engine.get_strava_sport_list(), 
                meta={ ct.SEVERITY: ct.WARNING }, 
                description= "Sport name exists in Strava refs", 
                ),
        ]    

        status, details = validate_dataframe(df_sport, "Sport_Data", hr_expectations)
        logger.info(f"End validating Sport data  : {processed_file_parquet}")
    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))



############################################
# HR EMPLOYEES + SPORT FUNCTIONS
############################################


def merge_hr_sport(hr_file_path: str, sport_file_path: str, output_file: str) -> None:
    """
    Performs a left join between HR data and sportive data based on employee ID.

    Args:
        hr_file_path: Path to the processed HR Parquet.
        sport_file_path: Path to the processed Sport Parquet.
        output_file: Path for the merged result.
    """
    shape = (0,0)
    status = ct.SUCCESS
    try :
        logger.info(f"Start mergin employees and sportive data  : {hr_file_path} + {sport_file_path}")

        df_hr = pd.read_parquet(hr_file_path)
        df_sport = pd.read_parquet(sport_file_path)
        df_final = df_hr.merge(df_sport,how='left',left_on="id", right_on="id")
        shape= df_final.shape
        df_final.to_parquet(output_file)
    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))



def validate_merge(hr_parquet: str, sport_parquet: str, merge_parquet: str) -> None:
    """
    Ensures data integrity after merging (no lost employees, identifying orphans).

    Args:
        hr_parquet: Path to reference HR data.
        sport_parquet: Path to reference Sport data.
        merge_parquet: Path to the merged file to validate.
    """

    details=[]
    status = ct.SUCCESS
    try :
        logger.info(f"Start validating merge file  : {merge_parquet}")

        df_hr = pd.read_parquet(hr_parquet)
        nb_employees:int = len(df_hr)

        df_sport = pd.read_parquet(sport_parquet)
        df_merge = pd.read_parquet(merge_parquet)
        nb_merge:int = len(df_merge)

        df_sports_test = df_sport.merge(df_merge, how= 'outer', on="id",indicator=True)
        df_missing_sports = df_sports_test[df_sports_test['_merge'] == 'right_only']
        nb_missing_sports:int = len(df_missing_sports)

        df_extra_sports = df_sports_test[df_sports_test['_merge'] == 'left_only']
        nb_extra_sports:int = len(df_extra_sports)
        #nb_extra_sports_emps = 

        df_extra_declared_sports = df_extra_sports[df_extra_sports['sport_type_y'].notnull()]
        nb_extra_declared_sports:int = len(df_extra_declared_sports)
        details = []
        status = ct.SUCCESS

        lost_employees = nb_employees-nb_merge
        if lost_employees:
            details.append(f'Expectation : Keep all {nb_employees} employees  in merge file {ct.FAILED} : {lost_employees} lost')
            status = ct.FAILED
        else:
            details.append(f'Expectation : Keep all {nb_employees} employees in merge file : {ct.SUCCESS}')

        if nb_missing_sports:
            details.append(f'Expectation : Each employee has id in sportive data : {ct.WARNING} : {nb_missing_sports} found')
            if status == ct.SUCCESS: status = ct.WARNING
        else:
            details.append(f'Expectation : Each employee has id in sportive data : {ct.SUCCESS}')
        
        if nb_extra_sports:
            details.append(f'Expectation : No orphan ids in sportive data : {ct.WARNING} : {nb_extra_sports} found')
            if status == ct.SUCCESS: status = ct.WARNING
        else:
            details.append(f'Expectation : No orphan ids in sportive data : {ct.SUCCESS}')

        if nb_extra_declared_sports:
            details.append(f'Expectation : No orphan ids with declared sport in sportive data : {ct.FAILED}')
            details.append(f"Invalid values list :\n  - ")

            for index, sport_row in df_extra_declared_sports.iterrows():
                details.append(f"{ct.SP4}- {sport_row['sport_type']}")
            status = ct.FAILED
        else:
            details.append(f'Expectation : No orphan ids with declared sport in sportive data : {ct.SUCCESS}')
    except Exception as e:
        status = ct.FAILED

    if status == ct.SUCCESS:
        logger.info(f"Succeed to validate merge data.")
    elif status == ct.WARNING:
        # SPECIFIC CASE : no errors except  ct.WARNINGS
        logger.warning(f"Warning during merge data validation")
    else:
        # CRITICAL CASE : Au moins une erreur 'critical' (comme l'ID)
        logger.error(f"Fail to validate merge data. : ")

    
    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))
    


def load_pg(merge_file_parquet: str) -> None:
    """
    Cleans final columns and loads the merged dataset into the PostgreSQL 'employees' table.

    Args:
        merge_file_parquet: Path to the final Parquet file to load.
    """
    
    details= []
    status = ct.SUCCESS
    try :
        logger.info(f"Start saving data to postgres  : {merge_file_parquet}")
        engine:Pse.Engine = create_engine(DATABASE_URL)

        df_merge = pd.read_parquet(merge_file_parquet)
        shape = (0,0)
        details = []
        cols_to_drop = ['margin_kms']

        df_merge.drop(columns = cols_to_drop, inplace= True)
        details.append(f"Remove cols : {' - '.join(cols_to_drop)}")
        details.append(f"Merge loading to PostgreSQL")
        details.append("Fields names : " + " - ".join(df_merge.columns.to_list()))
        ct.kestra_output('detail', details, f"{ct.SP2}- ", lf=True)
        try : 
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE employees")) 
                df_merge.to_sql('employees', conn, if_exists='append', index=False)
                shape = df_merge.shape
        except Exception as e:
            logger.error(f"Error during loading {merge_file_parquet} to postgreSQL: {e}")
            status = ct.FAILED
            
    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", (0,0), sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))




############################################
# CLI ENDPOINT
############################################


if __name__ == "__main__":
    # KESTRA must avoid this, calling functions directly
    # Allow CLI debugging
    import sys
    # Usage: python ingest_hr.py <action> <source> <destination>
    action = sys.argv[1]
    #ct.MAX_ROWS = 5
    BASE_DIR = Path(__file__).parent.parent.parent
    ct.KESTRA_MODE = False

    if action == "extract-hr":
        load_hr_xlsx(f"{BASE_DIR}/data/sources/Données+RH.xlsx", f"{BASE_DIR}/kestra/tmp/hr_raw.parquet")
    elif action == "transform-hr":
        transform_hr(f"{BASE_DIR}/kestra/tmp/hr_raw.parquet", f"{BASE_DIR}/kestra/tmp/hr_processed.parquet")
    elif action == "validate-hr":
        validate_hr(f"{BASE_DIR}/kestra/tmp/hr_processed.parquet")
    if action == "extract-sport":
        load_sport_xlsx(f"{BASE_DIR}/data/sources/Données+Sportive.xlsx", f"{BASE_DIR}/kestra/tmp/sport_raw.parquet")
    elif action == "transform-sport":
        transform_sport(f"{BASE_DIR}/kestra/tmp/sport_raw.parquet", f"{BASE_DIR}/kestra/tmp/sport_processed.parquet", f"{BASE_DIR}/data/sources/strava_sports.xlsx")
    elif action == "validate-sport":
        validate_sport(f"{BASE_DIR}/kestra/tmp/sport_processed.parquet", f"{BASE_DIR}/data/sources/strava_sports.xlsx")
    elif action == "merge":
        merge_hr_sport(f"{BASE_DIR}/kestra/tmp/hr_processed.parquet",f"{BASE_DIR}/kestra/tmp/sport_processed.parquet",f"{BASE_DIR}/kestra/tmp/merge.parquet")        
    elif action == "validate-merge":
        validate_merge(f"{BASE_DIR}/kestra/tmp/hr_processed.parquet", f"{BASE_DIR}/kestra/tmp/sport_processed.parquet",f"{BASE_DIR}/kestra/tmp/merge.parquet")
    elif action == "load-pg":
        load_pg(f"{BASE_DIR}/kestra/tmp/merge.parquet")                