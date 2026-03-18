import logging
import sys, os
import pandas as pd
import numpy as np
import random
from kestra import Kestra 
from datetime import datetime, timedelta
import calendar
from typing import List, Dict, Any, Tuple, Optional, cast
from cryptography.fernet import Fernet

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout  
)
logger = logging.getLogger("sds.infra.common_tools")

# --- Constants & Mappings ---
FAILED = "❌&nbsp;Failed"
WARNING = "⚠️&nbsp;Warning"
SUCCESS = "✅&nbsp;Succes"
SP2 = "&nbsp;"*2
SP4 = "&nbsp;"*4



SPORT_MAPPING: Dict[str, Any] = {
    'id': np.int32,
    'sport_type': object
}

SPORT_COLUMNS: List[str] = list(SPORT_MAPPING.keys())

WEEK: str = "week"
WEEKEND: str = "weekend"
VACATION: str = "vacation"
WEIGHT: str = "weight"
HOUR_RANGES: str = "hour_range"

START_DATE: str = "2025-06-01"

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
        Kestra.outputs({name: " ".join(values)})
        return
    
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
    log_text = str(final_txt).replace("&nbsp;", " ")
    logger.info(f"\n{name}: {log_text}")


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

class Sport_engine():
    """
    Core engine to handle sport activity logic, including duration, 
    distance calculations, and realistic scheduling.
    """
    def __init__(self, excel_sport_file, start_date) -> None:
        """
        Initializes the engine by loading sport configurations.
        
        Args:
            excel_sport_file: Path to the CSV containing sport metrics and popularity.
            start_date_str: The baseline date for activity generation (YYYY-MM-DD).
        """
        self.df: pd.DataFrame 
        self.sport_list = []
        self.strava_sport_list = []
        self.start_day = datetime.fromisoformat(start_date)
        self.load_sport_file(excel_sport_file)
       

    def make_aliases(self) -> None:
        """
        Parses the 'alias' column from the dataframe to create a lookup dictionary.
        Maps various sport synonyms to their official standardized name.
        """

        self.aliases ={}
        for _, row in self.df.iterrows():
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
            self.df = pd.read_excel(excel_sport_file)
            self.make_aliases()
        except:
            logger.error(f"Fail to load strava sports list : {excel_sport_file}")
            # En production, on pourrait isoler les lignes erronées ici
            raise FileNotFoundError(f"Fail to load strava sports list : {excel_sport_file}")
        return True

    def get_sports_list(self) -> List[str]:
        """
        Retrieves the full list of available sport names.

        Returns:
            A list of strings containing all sport names from the configuration.
        """

        if not len(self.sport_list):
            self.sport_list = self.df['sport'].to_list()
        return self.sport_list
    
    def get_strava_sport_list(self) -> List[str]:
        """
        Retrieves the list of sports existing in Strava references.

        Returns:
            A filtered list of strava-like sports .
        """
        if not len(self.strava_sport_list):
            self.strava_sport_list = self.df[self.df['strava_list']==1]['sport'].to_list()
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
        
        for sport in self.sport_list:
            if norm_sport == normalize_str(sport):
                return sport
        return sport_name

    
    def get_random_perfs(self, row: pd.Series, max_hours: int, max_repeat: int = 1) -> List[Dict[str, Any]]:
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
            duration_sec = random.randint(row['min_duration_min'] * 60 ,max_duration_sec )
            duration_ratio = duration_sec / mean_duration_sec
            mean_distance_meters = (row['max_distance_meters'] + row['min_distance_meters']) / 2
            distance_meters = round(mean_distance_meters * duration_ratio)
            perfs.append({'distance_meters':distance_meters,'duration_sec':duration_sec})
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
            delay_sec = random.randint(0,max_delay_sec)
            begin_date = datetime(chosen_year, chosen_month, chosen_date,min_start) + timedelta(days=i,seconds=delay_sec)
            end_date = begin_date + timedelta(seconds=duration_sec)
            perfs[i]['begin_date']= begin_date.timestamp()
            perfs[i]['end_date']= end_date.timestamp()

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
        row = cast(pd.Series, self.df.loc[sport_index])
        sport['sport'] = row['sport'] 

        period, time_slot = self.get_activity_random_period_timeslot(row)
        max_hours = time_slot[1]
        chosen_repeat = random.randint(a=1, b=row['vacation_repeat']) if period == VACATION else 1

        perfs = self.get_random_perfs(row,max_hours,chosen_repeat)

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
        index = self.df.loc[self.df['sport']== norm_name].index
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
            population=self.df.index,
            weights=self.df['popularity_score'].tolist(),
            k=1
        )[0]

        return self.get_random_activity_by_id(chosen_index)