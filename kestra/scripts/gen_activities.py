import pandas as pd
import numpy as np
import random
from faker import Faker
from kestra import Kestra
from pathlib import Path
import common_tools as ct
import fastparquet
import uuid
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, engine as Pse
import logging, os, sys


 #--- Configuration du Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  
    )
logger = logging.getLogger("sds.infra.gen_activities")

fake = Faker('fr_FR')

ACT_VS_INACT = 2
DATABASE_URL = os.getenv('DB_CONNECTION_STRING',"")


def run_generation(
    source_xlsx: str, 
    output_excel: str, 
    excel_sport_file: str,
    excel_locations_file:str, 
    num_rows: int = 1200
) -> None:

    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try :
        """
        Generates a synthetic dataset of sport activities based on employee profiles.
        
        The generation uses two different statistical distributions:
        - Power Law for active employees (defined in the source Excel).
        - Exponential distribution for inactive employees to simulate a realistic 
        adoption curve.
        
        Args:
            source_xlsx: Path to the Excel file containing employee data.
            output_parquet: Path where the generated Parquet file will be saved.
            excel_sport_file: Path to the CSV/Excel file containing sport configurations.
            num_rows: Total number of iterations to run for activity generation.
        """
        # 1. Load employee reference data
        # Standardizing names and types for join consistency
        
        df_ref: pd.DataFrame = pd.read_excel(
            source_xlsx, 
            names=ct.SPORT_COLUMNS, 
            dtype=ct.SPORT_MAPPING, 
            header=0
        )
        
        # Split employees into two groups: Active (declared sport) and Inactive
        mask_active = df_ref['sport_type'].notna()
        df_actifs = df_ref[mask_active]
        df_inactifs = df_ref[~mask_active]
        nb_active = len(df_actifs)
        nb_inactive = len(df_inactifs)
        
        # Initialize the Sport Engine (logic for dates, distances, and sport types)
        sport_engine = ct.Sport_engine(excel_sport_file, ct.START_DATE, excel_locations_file)
    
        # --- Weight calculation for ACTIVE group (Power Law) ---
        # Simulates a competitive environment where a few are very active
        raw_active_weights = 1 / np.power(np.arange(1, nb_active + 1), 0.4)
        np.random.shuffle(raw_active_weights)
        norm_active_weight = raw_active_weights / raw_active_weights.sum()

        
        # --- Weight calculation for INACTIVE group (Exponential) ---
        # Simulates a "long tail": many remain inactive, few become highly active
        raw_inactive_weight = np.random.exponential(scale=70, size=nb_inactive)
        norm_inactive_weight = raw_inactive_weight / raw_inactive_weight.sum()

        nb_created: int = 0
        final_activities: List[Dict[str, Any]] = []

        # 2. Main Generation Loop
        while nb_created < num_rows:
        
            # Probabilistic selection: active have ACT_VS_INACT more chance to pick from the Active pool
            
            ratio = (ACT_VS_INACT * nb_active) / (nb_active + nb_inactive)
            if random.random() < ratio and not df_actifs.empty:
                # Choix parmi les sportifs avec distribution Puissance
                emp = random.choices(
                    population=df_actifs.index.tolist(),
                    weights=norm_active_weight,
                    k=1
                )[0]
                selected_emp_row = df_actifs.loc[emp]
            else:
                emp = random.choices(
                    population=df_inactifs.index.tolist(),
                    weights=norm_inactive_weight,
                    k=1
                )[0]
                selected_emp_row = df_inactifs.loc[emp]

            fav_sport = selected_emp_row['sport_type']
            cur_activity={}
            cur_activity['employee_id'] = selected_emp_row['id']

            # Determine activity type: 85% chance to perform favorite sport if available
            if random.random() < 0.85 and isinstance(fav_sport, str) :
                activity = sport_engine.get_random_activity_by_name(fav_sport)
            else:
                activity = sport_engine.get_random_activity()
            cur_activity['sport'] = activity['sport'] 
            cur_activity['location'] = activity['location'] 

            # Map activity performances (dates, distances) to the current employee
            for perf in activity['perfs']:
                new_activity = cur_activity.copy()
                new_activity['distance_meters'] = perf['distance_meters']
                new_activity['begin_date'] = perf['begin_date']
                new_activity['duration_sec'] = perf['duration_sec']
                new_activity['id'] = ct.make_activity_id(perf['begin_date'])
                final_activities.append(new_activity)
                nb_created +=1

        # 3. Data persistence and Kestra output
        df_result = pd.DataFrame(final_activities)
        shape = df_result.shape
        df_result.sort_values('id', inplace=True)
        df_result.to_excel(output_excel, index=False)
        logger.info( f"End of fake generation : {ct.STATUS_TXT[status]}")
        # Integration with Kestra context
#        ct.kestra_output('activities_path',  output_excel)
    except Exception as e:
        status = ct.FAILED
        logger.error( f"Critical error : {e}")

    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))


def gen_comments(rows):
    pass


def temp_def():

    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try :
        details.append("")

    except Exception as e:
        status = ct.FAILED

    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))



def load_pg(processed_file: str, truncate_str: str) -> None:
    """
    Loads the generated into the PostgreSQL 'activities' table.

    Args:
        processed_file: Path to the final Parquet file to load.
        truncate: Boolean indicates if truncating before
    """
    truncate = truncate_str == "YES"

    logger.info(f"Start saving data to postgres  : {processed_file}")
    engine:Pse.Engine = create_engine(DATABASE_URL)

    df_activities = pd.read_parquet(processed_file)
    # need to reformat in datetime for pg
    df_activities['begin_date'] = pd.to_datetime(df_activities['begin_date'], unit='s', errors='coerce')
#    df_activities['end_date'] = pd.to_datetime(df_activities['end_date'], unit='s', errors='coerce')
    
    
    try : 
        with engine.begin() as conn:
            if truncate:
                conn.execute(text("TRUNCATE TABLE sports_activities")) 
            df_activities.to_sql('sports_activities', conn, if_exists='append', index=False)
            ct.kestra_output("shape", df_activities.shape, sep= " X ", trail= False)
            ct.kestra_output("status", ct.SUCCESS)

    except Exception as e:
        logger.error(f"Error during loading {processed_file} to postgreSQL: {e}")
        raise ConnectionError(f"Error during postgreSQL injection (Critical error).")

def fingerprint_activities(incoming_xlsx, output_file):

    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try : 
        details = []
        logger.info(f"Start connection to postgres  : ")
        engine:Pse.Engine = create_engine(DATABASE_URL)
        df_incoming = ct.extract_xlsx(incoming_xlsx, names= ct.ACTIVITY_COLUMNS)
        df_incoming['begin_date'] = pd.to_datetime(df_incoming['begin_date'])
#        df_incoming['id'] = df_incoming.apply(lambda row: ct.make_activity_id(row['begin_date']) if pd.isna(row['id']) else row['id'], axis=1)

        df_incoming['fingerprint'] = pd.util.hash_pandas_object(df_incoming[ct.ACTIVITY_FINGERPRINT], index=False).astype(str)
        df_incoming.to_parquet(output_file, index=False)

    except Exception as e:
        logger.error(f"Error during loading {incoming_xlsx} to postgreSQL: {e}")
        status = ct.FAILED
    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))
    

def scan_new_activities(incoming_activities,comparison_file:str):

    logger.info(f"Start loading incoming data data  : {incoming_activities}")

    bypass = "NO"
    details = []
    status = ct.SUCCESS
    try : 
        engine:Pse.Engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
                #conn.execute(text("TRUNCATE TABLE sports_activities")) 
            df_pg = pd.read_sql(sql='SELECT id, fingerprint FROM sports_activities', con = conn)
            details.append(f"Output file : #{incoming_activities}#")

        df_inc_activities = pd.read_parquet(incoming_activities)
        logger.info(f"Start connection to postgres  : ")
        df_comparison = df_inc_activities[['id', 'fingerprint']].merge(
            df_pg[['id', 'fingerprint']], 
            on=['id', 'fingerprint'], 
            how='outer', 
            indicator=True
        )
        new_activities = df_comparison[df_comparison['_merge'] == 'left_only']
        nb_news = len(new_activities)
        removed_activities= df_comparison[df_comparison['_merge'] == 'rigft_only']
        nb_removed = len(removed_activities)
        details.append(f"Removed activities : {nb_removed}")
        details.append(f"Added activities : {nb_news}")
        nb_changed = 0
        
        bypass = "YES" if (nb_removed + nb_news) >0 else "NO"
        if bypass == "YES":
            df_comparison.to_excel(comparison_file)
        if nb_removed + nb_changed >0:
            status = ct.WARNING    
    
    except Exception as e:
        logger.error(f"Error during loading {incoming_activities} to postgreSQL: {e}")
        statuts = ct.FAILED

    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("detail", details, lf= True,sep= f"{ct.SP2}- ", trail=True)
    ct.kestra_output("continue", bypass, raw=True)
    ct.kestra_output("status", status, raw=True)

def transform_activities(incoming_activities_parquet: str, comparision_file:str, output_file: str, excel_sport_file: str) -> None:
    """
    Ingestes activities comming from stra

    Args:
        raw_file_parquet: Path to the raw sportive Parquet file.
        output_file: Path for the normalized Parquet file.
        excel_sport_file: Path to the sports reference Excel file.
    """
    
    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try :
        logger.info(f"Start transforming sport raw data from : {incoming_activities_parquet}")
        
        df_incoming = pd.read_parquet(incoming_activities_parquet)

        details.append(f"Reads incoming activities data")
        engine:Pse.Engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            df_pg = pd.read_sql('sports_activities', conn)


        status = ct.SUCCESS
    
    except Exception as e:
        status = ct.FAILED
        logger.error(f"Error during loading employees table from postgreSQL: {e}")

    logger.info(f"End transforming incoming activities data into {output_file}")
    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))



if __name__ == "__main__":
    import sys
    
    action = sys.argv[1]
    # CLI parameters
    BASE_DIR = Path(__file__).parent.parent.parent
    ct.KESTRA_MODE = False
    if action == "rung_gen":
        run_generation(f"{BASE_DIR}/data/sources/Données+Sportive.xlsx", f"{BASE_DIR}/data/tmp/fake_activities.xlsx", f"{BASE_DIR}/data/sources/strava_sports.xlsx", f"{BASE_DIR}/data/sources/locations.xlsx", 500)
    elif action == "load_pg":
        load_pg(f"{BASE_DIR}/data/tmp/activities_processed.parquet", truncate_str= "NO")
    elif action == "fingerprint":
        fingerprint_activities(f"{BASE_DIR}/data/tmp/activities.xlsx", f"{BASE_DIR}/data/tmp/activities_income.parquet")
    elif action == "scan":
        scan_new_activities( f"{BASE_DIR}/data/tmp/activities_income.parquet",f"{BASE_DIR}/data/tmp/comparison.xlsx")
    elif action == "transform":
        transform_activities(f"{BASE_DIR}/data/tmp/activities_income.parquet",f"{BASE_DIR}/data/tmp/comparison.xlsx",f"{BASE_DIR}/data/tmp/activities_processed.parquet", f"{BASE_DIR}/data/sources/strava_sports.xlsx")        