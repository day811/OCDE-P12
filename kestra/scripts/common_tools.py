import logging
import sys, os, io, warnings
import pandas as pd
import numpy as np
import random
from kestra import Kestra 
from datetime import datetime, timedelta
import calendar
from typing import List, Dict, Any, Tuple, Optional, cast
from cryptography.fernet import Fernet
import great_expectations as gx

# --- Logging Configuration ---

#logger = logging.getLogger("sds.infra.common_tools")
logger:logging.Logger
# --- Constants & Mappings ---
SEVERITY = "severity"
FAILED = "FAILED"
WARNING = 'WARNING'
SUCCESS= 'SUCCESS'
KEEP_ROW = "Keep Row"
SP2 = "&nbsp;"*2
SP4 = "&nbsp;"
STATUS_TXT = {SUCCESS : "✅&nbsp;Success", WARNING : "⚠️&nbsp;Warning", FAILED : "❌&nbsp;Failed"}
EXTRA_INFO = "substitute"
INDEX = "#IDX#"


SPORT_MAPPING: Dict[str, Any] = {
    'id': object,
    'sport_type': object
}
MAX_ROWS = None
# DATABASE MAPPING
HR_MAPPING = {
    'id' : object ,
    'last_name'  :object,
    'first_name' : object,
    'birthday' : None,
    'business_unit' : object,
    'entry_date' : None,
    'salary' : np.float64,
    'employement_contract' :object,
    'vacation_days': np.int32,
    'address' : object,
    'transport_mode' : object,
}
HR_COLUMNS = list(HR_MAPPING.keys())
# Columns as they are download from Gsheet
RAW_ACTIVITY_COLUMNS= ['employee_id','sport', 'situation','distance_meters','begin_date','duration_sec','id']
# Columns that got insert in fingerprint compute
ACTIVITY_FINGERPRINT= ['employee_id','sport', 'situation', 'distance_meters','begin_date','duration_sec']
# Columns as they are stored in DB postegreSQL
PG_ACTIVITY_COLUMNS= ['employee_id','sport', 'situation','distance_meters','begin_date','duration_sec','id', 'comment', 'fingerprint']

# Columns as they are send in to RAG for generating comment
RAG_ACTIVITY_COLUMNS= ['id','name','sport', 'situation','performance' ]

SPORT_COLUMNS: List[str] = list(SPORT_MAPPING.keys())

WEEK: str = "week"
WEEKEND: str = "weekend"
VACATION: str = "vacation"
WEIGHT: str = "weight"
HOUR_RANGES: str = "hour_range"

FIRST_MONTH = os.getenv('FIRST_MONTH',"06")

# Weighting and time slots for activity generation
PERIOD_REPARTION: Dict[str, Dict[str, Any]] = {
    VACATION: {WEIGHT: 10, HOUR_RANGES: [(8, 14)]},
    WEEKEND: {WEIGHT: 50, HOUR_RANGES: [(8, 6), (14, 6)]},
    WEEK: {WEIGHT: 40, HOUR_RANGES: [(6, 2), (12, 2), (18, 4)]},
}

VACATION_MONTHS: List[int] = [2, 4, 7, 8, 10, 12]

KESTRA_MODE = True
SP2 = "&nbsp;"*2
SP4 = "&nbsp;"*4

CRYPT_KEY= os.getenv('CRYPT_KEY')
cipher_suite = Fernet(CRYPT_KEY.encode()) # type: ignore

def make_activity_id(activity_date:datetime):

    return f"ACT-{activity_date.strftime('%y%m%d-%H%M')}-{random.randint(0, 99999):05d}"

def make_exit_status(status):
    exit_status = 0 if status != FAILED else 1
      
    return exit_status

def extract_xlsx( file_path: str,  names= None, mapping= None, header=0, max_rows = MAX_ROWS) -> pd.DataFrame:
    """
    Extracts raw data from HR or Sport Excel files and converts them to Parquet format.
    Includes encryption for sensitive HR fields (names and addresses).

    Args:
        file_type: Type of file to process ("hr" or "sport").
        file_path: Path to the source Excel file.
        output_file: Destination path for the generated Parquet file.
    """

    try :
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")        
            df = pd.read_excel(file_path, names= names, dtype= mapping,header=header,nrows=max_rows,engine='openpyxl')
    except Exception as e:
        logger.error(f"Error during loading {file_path}: {e}")
        raise FileNotFoundError(f"File {file_path} not found (Critical error).")

    logger.info(f"End extracting HR data from : {file_path}")
    return df

def encrypt_text(plain_text: Any) -> Optional[str]:
    """
    Encrypts a plain text or ID into an unreadable cipher string using Fernet.

    Args:
        plain_text: The data to be encrypted (converted to string internally).

    Returns:
        The encrypted string or None if the input is None.
    """

    if plain_text is None: return None
    # Fernet travaille avec des bytes, on convertit l'ID
    encrypted_text = cipher_suite.encrypt(str(plain_text).encode())
    return encrypted_text.decode()

def decrypt_text(encrypted_text: Optional[str]) -> Optional[str]:
    """
    Decrypts a Fernet-encrypted string back into its original plain text format.

    Args:
        encrypted_text: The cipher text to decrypt.

    Returns:
        The original plain text string, or an error message if decryption fails.
    """
    
    if encrypted_text is None: return None
    try:
        decrypted_text = cipher_suite.decrypt(encrypted_text.encode())
        return decrypted_text.decode()
    except Exception as e:
        return f"Error: {e}"
    
def kestra_output(name: str, values: Any, sep: str = "", lf: bool = False, trail: Any = True, raw = False) -> None:
    """
    Handles variable output for Kestra context or standard CLI printing.

    Args:
        name: The name of the output variable in Kestra.
        values: The data to output (string or list of strings).
        sep: Separator used between multiple values.
        lf: If True, adds a line feed and indentation before the separator.
        trail: Controls leading/trailing text or spacing (True for default separator).
    """
    if not isinstance(values, list):
        values = [str(values)]
    if raw:
        final_txt =" ".join(values)
    else:
        if isinstance(trail,str):
            final_txt = trail
        elif trail == True:
            final_txt = sep
        else:
            final_txt = ""
        
        if lf:
            lf_txt = f"{SP2}\n" 
            sep = lf_txt + sep
        else:
            lf_txt=""

        final_txt += sep.join([ str(value) for value in values])
        final_txt += lf_txt + SP2
    
    if KESTRA_MODE :
        Kestra.outputs({name: final_txt})
    if not raw :
        final_txt = str(final_txt).replace("&nbsp;", " ")
    logger.info(f"\n{name}: {final_txt}")


def normalize_str(text: str) -> str:
    """
    Normalizes a string by removing accents, converting to lowercase, 
    and replacing separators (hyphens/slashes) with spaces.

    Args:
        text: The raw string to normalize.

    Returns:
        The cleaned, lowercase, and accent-free string.
    """

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




def validate_dataframe(df: pd.DataFrame, suite_name: str, expectations_list: List[Any]) -> Tuple[pd.DataFrame,str, List[str]]:
    """
    Generic Great Expectations validation function using the WAP (Write-Audit-Publish) pattern.

    Args:
        df: The DataFrame to validate.
        suite_name: Name of the expectation suite.
        expectations_list: List of GX expectation objects to run.

    Returns:
        A tuple containing the global status (SUCCESS/WARNING/CRITICAL) and detailed logs.
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
  
    
    
    status = SUCCESS
    details = []
    masks_to_delete= []
    for result in validation_result.results:
        severity = result.expectation_config.meta.get(SEVERITY, FAILED)  # type: ignore
        keep_row = result.expectation_config.meta.get(KEEP_ROW, True) # type: ignore
        column_name = result.expectation_config.kwargs.get("column") # type: ignore
        substitute = result.expectation_config.meta.get(EXTRA_INFO, 'id') # type: ignore
        if result.success:
           details.append(f"Expectation : {result.expectation_config.get('description', '')} ({severity}): {SUCCESS}")  # type: ignore
        else:
            flaws = result.result.get('partial_unexpected_list',[])
            flaws_index = result.result.get('partial_unexpected_index_list',[])

            if severity == FAILED:
                status = FAILED
                details.append(f"Expectation : {result.expectation_config.get('description',)}  ({severity}): {FAILED}")  # type: ignore
            else:
                details.append(f"Expectation : {result.expectation_config.get('description',)}  ({severity}): {WARNING}")  # type: ignore
                if status ==  SUCCESS: status = WARNING

            if len(flaws): 
                if len(flaws_index):
                    # list of flawed indexes exists
                    flaw_output = f"{SP4}Invalid values list :"
                    if not keep_row:
                        #indexes_to_delete.add(int(index_flaw))    
                        flaw_dict = { 'column': column_name, 'values': flaws_index, EXTRA_INFO:substitute, 'is_index' : True}
                        masks_to_delete.append(flaw_dict) 
                    for index_flaw, flaw in zip(flaws_index,flaws):
                        reference = f"{str(df.loc[index_flaw, substitute])}"
                        flaw_output +=  f"\n{SP4}- Ref : {reference} --> Lignes -> {str(index_flaw)}"  

                else:
                #no index, only a list of value that don't respect the column expectation
                    flaw_output = f"- Invalid values list :\n{SP4}- "
                    flaw_output +=  f"\n{SP4}- {column_name} : ".join([str(flaw) for flaw in flaws])
                    if not keep_row:
                        flaw_dict = {'column' : column_name, 'values': flaws, EXTRA_INFO:substitute}
                        masks_to_delete.append(flaw_dict)    

                details.append(f'{flaw_output}')
    
    df_cleaned = pd.DataFrame()
    if status != FAILED:
        if len(masks_to_delete):
            for delete_mask in masks_to_delete:
                column_to_mask = delete_mask['column']
                is_index = delete_mask.get('is_index', False)
                if is_index:
                    mask = df.index.isin(delete_mask['values'])
                else:
                    mask = df[column_to_mask].isin(delete_mask['values'])
                substitute = delete_mask[EXTRA_INFO]
                fields = {substitute, column_to_mask}
                to_delete = df[mask][list(fields)].to_dict('records')
                nb_to_delete = len(to_delete)
                details.append(f"Deleted {nb_to_delete} flawed activities : ")
                details.extend(to_delete)
                df_cleaned = df[~mask].copy()
        else:
            df_cleaned = df.copy()

    if status == SUCCESS:
        logger.info(f"Succeed to validate {suite_name} data with gX.")
    elif status == WARNING:
        # SPECIFIC CASE : no errors except  WARNINGS
        logger.warning(f"Warning during {suite_name} validation with gX")
    else:
        # CRITICAL CASE : Au moins une erreur 'critical' (comme l'ID)
        logger.error(f"Fail to validate {suite_name} with gX (Critical). : ")
    return df_cleaned, status, details

class Sport_engine():
    """
    Core engine to handle sport activity logic, including duration, 
    distance calculations, and realistic scheduling.
    """
    def __init__(self, excel_sport_file, first_month, excel_locations_file: str = "") -> None:
        """
        Initializes the engine by loading sport configurations.
        
        Args:
            excel_sport_file: Path to the CSV containing sport metrics and popularity.
            start_date_str: The baseline date for activity generation (YYYY-MM-DD).
        """
        self.df_sport: pd.DataFrame
        self.df_locations: pd.DataFrame 
        self.sport_list = []
        self.strava_sport_list = []
        tnow = datetime.now()
        chosen_month= int(first_month)
        chosen_year = tnow.year if int(first_month)< tnow.month else tnow.year - 1
        self.start_day = datetime(chosen_year,chosen_month,1)
        self.load_sport_file(excel_sport_file)
        if excel_locations_file:
            self.load_locations_file(excel_locations_file)
       

    def make_aliases(self) -> None:
        """
        Parses the 'alias' column from the dataframe to create a lookup dictionary.
        Maps various sport synonyms to their official standardized name.
        """

        self.aliases ={}
        for _, row in self.df_sport.iterrows():
             if isinstance(row['alias'],str):
                 for alias in row['alias'].split(','):
                     self.aliases[alias.strip()]  = row['sport']


    def load_sport_file(self, excel_sport_file: str) -> bool:
        """
        Loads and validates the sports configuration from an Excel file.

        Args:
            excel_sport_file: Path to the Excel file to load.

        Returns:
            True if the file is loaded successfully.

        Raises:
            FileNotFoundError: If the file cannot be accessed or parsed.
        """

        logger.info(f"Start loading Strava Sports List  : {excel_sport_file}")
    
        try:
    #        global SPORTS_LIST
            self.df_sport = pd.read_excel(excel_sport_file)
            self.make_aliases()
            self.df_sport['templates'] = self.df_sport['templates'].str.split('|')
        except:
            logger.error(f"Fail to load strava sports list : {excel_sport_file}")
            # En production, on pourrait isoler les lignes erronées ici
            raise FileNotFoundError(f"Fail to load strava sports list : {excel_sport_file}")
        return True

    def load_locations_file(self, excel_locations_file: str) -> bool:
        """
        Loads and validates the sports configuration from an Excel file.

        Args:
            excel_sport_file: Path to the Excel file to load.

        Returns:
            True if the file is loaded successfully.

        Raises:
            FileNotFoundError: If the file cannot be accessed or parsed.
        """

        logger.info(f"Start loading Strava Sports List  : {excel_locations_file}")
    
        try:
    #        global SPORTS_LIST
            self.df_locations = pd.read_excel(excel_locations_file)
        except:
            logger.error(f"Fail to load locations list : {excel_locations_file}")
            # En production, on pourrait isoler les lignes erronées ici
            raise FileNotFoundError(f"Fail to load locations list : {excel_locations_file}")
        return True

    def get_sports_list(self) -> List[str]:
        """
        Retrieves the full list of available sport names.

        Returns:
            A list of strings containing all sport names from the configuration.
        """

        if not len(self.sport_list):
            self.sport_list = self.df_sport['sport'].to_list()
        return self.sport_list
    
    def get_strava_sport_list(self) -> List[str]:
        """
        Retrieves the list of sports existing in Strava references.

        Returns:
            A filtered list of strava-like sports .
        """
        if not len(self.strava_sport_list):
            self.strava_sport_list = self.df_sport[self.df_sport['strava_list']==1]['sport'].to_list()
        return self.strava_sport_list

    def get_normalized_sport(self, sport_name: Optional[str]) -> Optional[str]:
        """
        Standardizes a given sport name by checking against aliases and normalized names.

        Args:
            sport_name: The raw sport name string to identify.

        Returns:
            The official sport name if found, otherwise the original string (normalized).
        """

        if not sport_name:  
            return None
        norm_sport = normalize_str(sport_name)

        if norm_sport in self.aliases.keys():
            return self.aliases[norm_sport]
        
        for sport in self.get_sports_list():
            if norm_sport == normalize_str(sport):
                return sport
        return None

    def get_random_situation(self, row, location):
        if random.random() < 0.40:
            templates =  row['templates']          
            # On pioche un template selon le sport, sinon une phrase générique
#            templates = SITUATIONS_TEMPLATES.get(sport_name, ["Séance à {loc}", "Top moment vers {loc}"])
            return random.choice(templates).format(loc=location)
        else:                       
            return ""

        
        
        location = ""

    def get_random_perfs(self, row: pd.Series, max_hours: int, location:str,max_repeat: int = 1) -> List[Dict[str, Any]]:
        """
        Generates random distance and duration metrics based on sport constraints.

        Args:
            row: The pandas Series containing sport-specific min/max metrics.
            max_hours: The maximum duration allowed by the selected time slot.
            max_repeat: Number of performance sets to generate (for vacations).

        Returns:
            A list of dictionaries containing 'distance_meters' and 'duration_sec'.
        """

        # max duration can't exceed hours range for period
        max_duration_sec = min( max_hours * 60, row['max_duration_min'])*60
        mean_duration_sec = (row['max_duration_min'] + row['min_duration_min']) * 30
        perfs = []
        for i in range(max_repeat):   

            situation = self.get_random_situation (row, location)
            duration_sec = random.randint(row['min_duration_min'] * 60 ,max_duration_sec )
            duration_ratio = duration_sec / mean_duration_sec
            mean_distance_meters = (row['max_distance_meters'] + row['min_distance_meters']) / 2
            distance_meters = round(mean_distance_meters * duration_ratio * (1 + ((random.randint(-100,100)/500))))
            perfs.append({'distance_meters':distance_meters,'duration_sec':duration_sec, 'situation' : situation})
        return perfs

    def get_activity_random_period_timeslot(self, row: pd.Series) -> Tuple[str, Tuple[int, int]]:
        """
        Determines a random time period (Week, Weekend, Vacation) and an associated hour range.

        Args:
            row: The pandas Series containing availability flags for the sport.

        Returns:
            A tuple containing the period name (str) and the hour range tuple (start_hour, duration).
        """

        period_list = [when for when in PERIOD_REPARTION.keys() if row[when] == 1 ]
        period_weight = [when[WEIGHT] for index , when in PERIOD_REPARTION.items() if row[index] == 1]
        
        period = random.choices(
            population=period_list,
            weights=period_weight,
            k=1
        )[0]
        hour_list =  PERIOD_REPARTION[period][HOUR_RANGES]
        timeslot = random.choices(population=hour_list, k=1)[0]

        return (period, timeslot)

    def get_activity_random_dates(self, perfs: List[Dict[str, Any]], period: str, 
                                  hour_range: Tuple[int, int], vacation_repeat: int) -> List[Dict[str, Any]]:
        """
        Calculates realistic start and end dates for activities based on a specific period.

        Args:
            perfs: List of performance metrics to be assigned to specific dates.
            period: The selected period type (week, weekend, vacation).
            hour_range: Tuple defining the allowed (start_hour, duration_window).
            vacation_repeat: Number of consecutive days for vacation activities.

        Returns:
            The input list updated with 'begin_date' and 'end_date' timestamps.
        """

        ref_year = self.start_day.year
        ref_month= self.start_day.month
        if period == VACATION:
            chosen_month = int(random.choices(population=VACATION_MONTHS, k=1)[0])

        else:
            chosen_month = random.randint(a=1,b=12)

        chosen_year = ref_year + (1 if chosen_month <= ref_month else 0)
        first_day, days_in_month = calendar.monthrange(chosen_year, chosen_month)

        if period == WEEK:
            chosen_day= random.randint(a=0,b=4)
        elif period == WEEKEND:
            chosen_day= random.randint(a=5,b=6)
        else:
            chosen_day= random.randint(a=0,b=6)

        first_possible_date = 1 + ( (chosen_day - first_day) %7)
        possible_dates = [i for i in range(first_possible_date,days_in_month+1 ,7)]

        chosen_date= random.choices(population=possible_dates, k=1)[0]
        min_start, nb = hour_range

        for i in range(0,vacation_repeat):
            duration_sec = perfs[i]['duration_sec']
            max_delay_sec = 3600 * nb - duration_sec
            delay_min = random.randint(0,max_delay_sec)//60
            begin_date = datetime(chosen_year, chosen_month, chosen_date,min_start) + timedelta(days=i,minutes=delay_min)
            perfs[i]['begin_date']= begin_date

        return perfs

    
    def get_random_activity_by_id(self, sport_index: int) -> Dict[str, Any]:
        """
        Generates a complete activity package using a specific row index from the sports database.

        Args:
            sport_index: The index of the row in the internal dataframe.

        Returns:
            A dictionary containing the sport name and a list of performance/date objects.
        """

        sport: Dict[str, Any] = {}
        row = cast(pd.Series, self.df_sport.loc[sport_index])
        sport['sport'] = row['sport'] 

        location_idx = row['location']
        if np.isnan(location_idx):
            location = random.choices(self.df_locations['location'].tolist(), k=1)[0]
        else:
            index= int(location_idx)
            population = self.df_locations[self.df_locations['index'] == index]['location'].tolist()
            location = random.choices(population, k=1)[0]

#        sport['location'] = location
        
        period, time_slot = self.get_activity_random_period_timeslot(row)
        max_hours = time_slot[1]
        chosen_repeat = random.randint(a=1, b=row['vacation_repeat']) if period == VACATION else 1

        perfs = self.get_random_perfs(row,max_hours, location, chosen_repeat)

        perfs = self.get_activity_random_dates( perfs, period, time_slot, chosen_repeat)
        sport['perfs'] = perfs
        return sport           
    

    def get_random_activity_by_name(self, raw_sport_name: str) -> Dict[str, Any]:
        """
        Fetches and generates activity data for a sport identified by its name.

        Args:
            raw_sport_name: The name (or alias) of the sport to generate.

        Returns:
            A complete activity dictionary, or an empty dictionary if the sport is unknown.
        """
        
        norm_name = self.get_normalized_sport(raw_sport_name)
        index = self.df_sport.loc[self.df_sport['sport']== norm_name].index
        if not index.empty:
            sport = self.get_random_activity_by_id(index[0])
            sport['sport'] = raw_sport_name
            return sport
        else:
            return {}


    def get_random_activity(self) -> Dict[str, Any]:
        """
        Selects a random sport based on popularity scores and generates its data.

        Returns:
            A complete activity dictionary for a weighted-randomly selected sport.
        """
        chosen_index = random.choices(
            population=self.df_sport.index,
            weights=self.df_sport['popularity_score'].tolist(),
            k=1
        )[0]

        return self.get_random_activity_by_id(chosen_index)