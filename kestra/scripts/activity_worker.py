import pandas as pd
import numpy as np
import random
from kestra import Kestra
from pathlib import Path
import common_tools as ct
import fastparquet
import json
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, engine as Pse
import logging, os, sys, time
import great_expectations as gx

SIMPLE_COMMENTS = os.getenv('SIMPLE_COMMENTS',"true").lower() != 'false'

 #---Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
    )
logger = logging.getLogger("sds.infra.ingest_activities")

logger.propagate = False # Avoid log doubling with base config
logger.setLevel(logging.INFO)

# Handler 1 : INFOS logs forward to STDOUT (Green in Kestra)
h_info = logging.StreamHandler(sys.stdout)
h_info.setLevel(logging.INFO)
h_info.addFilter(lambda record: record.levelno <= logging.INFO) # Stricte INFO

# Handler 2 : WARNINGS/ERRORS logs forward to STDERR (Orange/Red in Kestra)
h_warn = logging.StreamHandler(sys.stderr)
h_warn.setLevel(logging.WARNING) # Catch WARNING, ERROR, CRITICAL

# Format
formatter = logging.Formatter('%(levelname)s - %(message)s')
h_info.setFormatter(formatter)
h_warn.setFormatter(formatter)

logger.addHandler(h_info)
logger.addHandler(h_warn)

ct.logger = logger


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
        sport_engine = ct.Sport_engine(excel_sport_file, ct.FIRST_MONTH, excel_locations_file)
    
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

            # Map activity performances (dates, distances) to the current employee
            for perf in activity['perfs']:
                new_activity = cur_activity.copy()
                new_activity['situation'] = perf['situation']
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


def validate_incoming(incoming_xlsx: str, output_file):
    """
    Valide l'intégrité des données entrantes avant la comparaison.
    """
    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try : 
        details = []
        logger.info(f"Start connection to postgres  : ")
        engine:Pse.Engine = create_engine(DATABASE_URL)
        valid_ids=[]
        with engine.begin() as conn:
            logger.info(f"Start loadaing employees list from db  : ")
            df_emp_ids = pd.read_sql("SELECT id FROM employees", conn)
            valid_ids = df_emp_ids['id'].values            
        
        df_incoming = ct.extract_xlsx(incoming_xlsx, names= ct.RAW_ACTIVITY_COLUMNS)
        df_incoming['employee_id'] = df_incoming['employee_id'].astype(str)
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
                column= "employee_id", 
                value_set= valid_ids.tolist(),
                meta={ ct.SEVERITY: ct.WARNING, ct.KEEP_ROW : False }, 
                description= "Employee exists in db", 
                ),
            gx.expectations.ExpectColumnValuesToNotBeNull( # pyright: ignore[reportPrivateImportUsage]
                column= "sport", 
                description= "No missing sport", 
                meta={ ct.SEVERITY: ct.WARNING , ct.KEEP_ROW : False },
                ) ,
            gx.expectations.ExpectColumnValuesToNotBeNull( # pyright: ignore[reportPrivateImportUsage]
                column= "duration_sec", 
                description= "No missing duration", 
                meta={ ct.SEVERITY: ct.WARNING , ct.KEEP_ROW : False },
                ) ,
            gx.expectations.ExpectColumnValuesToNotBeNull( # pyright: ignore[reportPrivateImportUsage]
                column= "begin_date", 
                description= "No missing begin date", 
                meta={ ct.SEVERITY: ct.WARNING , ct.KEEP_ROW : False },
                ) ,
        ]    
        df_cleaned, status, details= ct.validate_dataframe(df_incoming, "Incoming Data", hr_expectations)

        shape= df_cleaned.shape
        df_cleaned.to_parquet(output_file, index= False)
        details.append(f"Save validated activities to {output_file} : {ct.SUCCESS}")

    except Exception as e:
        logger.error(f"Error during validating {incoming_xlsx} to postgreSQL: {e}")
        status = ct.FAILED
    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))


def fingerprint_activities(incoming_activities, output_file):

    details = []
    status = ct.SUCCESS
    shape = (0,0)
    try : 
        details = []
        logger.info(f"Start connection to postgres  : ")
        df_incoming = pd.read_parquet(incoming_activities)
        df_incoming['begin_date'] = pd.to_datetime(df_incoming['begin_date'])

        df_incoming['fingerprint'] = pd.util.hash_pandas_object(df_incoming[ct.ACTIVITY_FINGERPRINT], index=False).astype(str)
        df_incoming.to_parquet(output_file, index=False)
        logger.info(f"Save fingerprinted activities to {output_file}")
        shape = df_incoming.shape
    except Exception as e:
        logger.error(f"Error during loading {incoming_activities} to postgreSQL: {e}")
        status = ct.FAILED
#    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))




def scan_new_activities(fingerprinted_activities,actions_file:str):


    continue_flow = "NO"
    details = []
    status = ct.SUCCESS
    shape=(0,0)
    try : 
        logger.info(f"Start loading incoming data : {fingerprinted_activities}")
        df_inc_activities = pd.read_parquet(fingerprinted_activities)
        df_inc_activities['employee_id'] = df_inc_activities['employee_id'].astype(str)
        df_inc_activities['distance_meters'] = df_inc_activities['distance_meters'].astype('Int64')
        df_inc_activities['duration_sec'] = df_inc_activities['duration_sec'].astype('Int64')

        nb_incoming = len(df_inc_activities)
        engine:Pse.Engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            logger.info(f"Start connection to postgres  : ")
            df_pg = pd.read_sql(sql='SELECT * FROM sports_activities', con = conn)
            logger.info(f"Postrgre activities loaded")

        df_pg['employee_id'] = df_pg['employee_id'].astype(str)
        df_pg['distance_meters'] = df_pg['distance_meters'].astype('Int64')
        df_pg['duration_sec'] = df_pg['duration_sec'].astype('Int64')

        df_actions = df_inc_activities.merge(
            df_pg, 
            on=['id', 'fingerprint'], 
            how='outer', 
            indicator=True,
            suffixes= ('','_y')
        )

        details.append(f"Number of Incoming data activities : {nb_incoming}")

        mask = df_actions['_merge'] == 'both'
        nb_identical = len(df_actions[mask])
        details.append(f"Number of identical activities : {nb_identical}")
        df_diff = df_actions[df_actions['_merge'] != 'both'].copy()

        # remove _x and _y suffixes in column name
        cols_to_fix = [c.replace('_y', '') for c in df_diff.columns if c.endswith('_y')]

        mask_right = df_diff['_merge'] == 'right_only'
        for col in cols_to_fix:
            df_diff.loc[mask_right, col] = df_diff.loc[mask_right, f"{col}_y"]

        # 4. On supprime proprement les colonnes techniques
        df_diff.drop(columns=[f"{c}_y" for c in cols_to_fix], inplace=True)

        is_duplicated = df_diff.duplicated(subset=['id'], keep=False)
        df_diff['status'] = 'new' # Par défaut
        df_diff.loc[is_duplicated, 'status'] = 'modified'
        df_diff.loc[(df_diff['_merge'] == 'right_only') & (~is_duplicated), 'status'] = 'deleted'

        df_final = df_diff.sort_values('_merge', ascending=True)

        nb_news = len(df_final[df_final['status'] == 'new'])
        details.append(f"Number of new activities : {nb_news}")

        nb_deleted  = len(df_final[df_final['status'] == 'deleted'])
        details.append(f"Number of removed activities : {nb_deleted}")
       
        nb_modified= len(df_final[(df_final['status'] == 'modified') & (df_final['_merge'] == 'left_only')])
        details.append(f"Number of modified activities : {nb_modified}")
        
        continue_flow = "YES" if (nb_deleted + nb_news + nb_modified) >0 else "NO"

        if continue_flow == "YES":
            try :
#                df_final.drop(columns='comment', inplace=True)
                df_final.to_parquet(actions_file, index=False)
                details.append(f"Activity actions saved to file : {actions_file}")
                shape = df_final.shape
                if nb_deleted + nb_modified >0:
                    status = ct.WARNING    
                    logger.warning(f"Anomalies found during analysis of {actions_file} ")
            except Exception as e:
                logger.error(f"Error while saving {actions_file} : {e}")
                status = ct.FAILED
        
    except Exception as e:
        logger.error(f"Error during scanning {fingerprinted_activities} : {e}")
        status = ct.FAILED

    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("detail", details, lf= True,sep= f"{ct.SP2}- ", trail=True)
    ct.kestra_output("continue", continue_flow, raw=True)
    ct.kestra_output("status", status, raw=True)


def generate_comments(df_list:pd.DataFrame ):
    
    from rag.engine import RAGEngine as Rag

    def batch_comments(df_batch: pd.DataFrame, rag) -> List[str]:
        """
        Simule la génération de commentaires pour un batch.
        Plus tard, ici se fera l'appel au RAG.
        """
        # Simulation : "Sport à Lieu"
        # On gère les cas où situation pourrait être NaN
        # add suffixe as we work wtih full merge file
        response ={}
        if SIMPLE_COMMENTS:
            data = []
            for idx, row in df_batch.iterrows():
                data.append({ 'id': row['id'] ,'comment' : f"Bypass IA : {row['situation']}"})
            return data
        else:
            context=df_batch.to_dict(orient='records')
            response = rag.get_comments(context)
            clean_content = response.get('answer').replace('```json', '').replace('```', '')
            data = json.loads(clean_content)
        return data['results']

    details=[]
    batch_qty = 10
    rag=None
    if not SIMPLE_COMMENTS:
        rag = Rag()
    # On identifie les indices des lignes à traiter
    indices_todo = df_list.index
    nb_to_generate = len(indices_todo)
    logger.info(f"Début de la génération de {nb_to_generate} commentaires par batchs de {batch_qty}")

    comments= []
    for i in range(0, nb_to_generate, batch_qty):
        current_indices = indices_todo[i : i + batch_qty]
        
        # Extraction du sous-dataframe
        df_batch = df_list.loc[current_indices]
        
        comments_list = batch_comments(df_batch, rag=rag)
        time.sleep(0.5)
        comments.extend(comments_list)
        
        logger.info(f"Batch {i//batch_qty + 1} traité ({len(current_indices)} lignes)")
    df_comments = pd.DataFrame(comments)    

    return df_comments

def format_performance(row):
    distance_meters= row.get('distance_meters',0)
    dist_txt = ""
    if distance_meters :
        dist_km = round(row['distance_meters'] / 1000, 1)
        dist_txt = f"{dist_km} km en "
    
    hours = int(row['duration_sec'] // 3600)
    minutes = int((row['duration_sec'] % 3600) // 60)
    
    if hours > 0:
        duration_str = f"{hours}h{minutes:02d}"
    else:
        duration_str = f"{minutes} min"
        
    return f"{dist_txt}{duration_str}"

def comment_activities(actions_file:str, output_file: str, excel_sport_file: str) -> None:
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
        logger.info(f"Start loading actions to do : {actions_file}")
        df_actions = pd.read_parquet(actions_file)
        df_actions['employee_id'] = df_actions['employee_id'].astype(str)

        #isolate activities to be comment 'right_only must be kept
        mask_todo = (df_actions['_merge'] == 'left_only')
        df_to_comment = df_actions[mask_todo]
        engine:Pse.Engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            logger.info(f"Start loadaing employees list from db  : ")
            df_employees = pd.read_sql("SELECT id as employee_id , first_name , last_name FROM employees", conn)
            df_employees['employee_id'] = df_employees['employee_id'].astype(str)

        df_rag = df_to_comment.merge(df_employees, how= 'left', on = 'employee_id').copy()
        df_rag['first_name'] = df_rag['first_name'].apply(ct.decrypt_text)
        df_rag['last_name'] = df_rag['last_name'].apply(ct.decrypt_text)
        df_rag['name'] = df_rag['first_name']  + df_rag['last_name'].apply( lambda x : f" {x[0]}.")
        df_rag['performance'] = df_rag.apply(format_performance,axis=1)

        # make comments for changed or new activities
        df_commented = generate_comments(df_rag[ct.RAG_ACTIVITY_COLUMNS])
        # merge with full action df to keep old comments
        df_final = df_actions.merge(df_commented, on='id', how='left', suffixes= ('','_y'))
        
        mask_done = df_final['_merge'] == 'left_only'
        df_final.loc[mask_done, 'comment'] = df_final.loc[mask_done, 'comment_y']
        df_final['comment'] = df_final['comment'].apply(ct.encrypt_text)
        df_final.drop(columns='comment_y', inplace=True)
        df_final.to_parquet(output_file, index=False) 
        shape=df_final.shape
    
    except Exception as e:
        status = ct.FAILED
        logger.error(f"Error while generating incoming comments: {e}")

    logger.info(f"End incoming comments generation to file {output_file}")
    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))

def load_pg(processed_file: str) -> None:
    """
    Loads the generated into the PostgreSQL 'activities' table.

    Args:
        processed_file: Path to the final Parquet file to load.
        truncate: Boolean indicates if truncating before
    """
    details = []
    status = ct.SUCCESS
    shape = (0,0)
    logger.info(f"Start saving data to postgres  : {processed_file}")
    engine:Pse.Engine = create_engine(DATABASE_URL)

    df_activities = pd.read_parquet(processed_file)
    mask = (df_activities['status'] == 'modified') & (df_activities['_merge'] == 'right_only')
    df_todo = df_activities[~mask]
    df_memory = df_activities[mask].copy()
    try : 
        engine:Pse.Engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            logger.info(f"Start connection to postgres  : ")
            nb_new = nb_mod = nb_del = 0
            for _, row in df_todo.iterrows():
                params = row.to_dict()
                params_log = str({k: params[k] for k in ct.PG_ACTIVITY_COLUMNS if k in params})
                if row['status'] == 'new':
                    # INSERT : On utilise les noms de colonnes de la table
                    query = text("""
                        INSERT INTO sports_activities (id, employee_id, sport, situation, begin_date, duration_sec, distance_meters, comment, fingerprint)
                        VALUES (:id, :employee_id, :sport, :situation, :begin_date, :duration_sec, :distance_meters, :comment, :fingerprint)
                    """)
                    conn.execute(query, params) # type: ignore
                    nb_new += 1
                elif row['status'] == 'modified':
                    # UPDATE : On met à jour les champs qui peuvent changer
                    query = text("""
                        UPDATE sports_activities 
                        SET sport = :sport,
                            employee_id = :employee_id, 
                            situation = :situation, 
                            begin_date = :begin_date,
                            duration_sec = :duration_sec, 
                            distance_meters = :distance_meters,
                            comment = :comment,
                            fingerprint = :fingerprint
                        WHERE id = :id
                    """)
                    conn.execute(query, params) # type: ignore
                    old_row = df_memory.loc[df_memory['id']==row['id']].reset_index(drop=True)
                    old_txt = str(old_row.to_dict('records'))
                    details.append(f"{ct.WARNING} : modified activity :")
                    details.append(f"{ct.WARNING} : Old values : {old_txt}")
                    details.append(f"{ct.WARNING} : New values : {params_log}")
                    status = ct.WARNING
                    nb_mod += 1
                elif row['status'] == 'deleted':
                    # DELETE (ou log d'anomalie selon ton choix)
                    logger.warning(f"Deleting activity {row['id']} from DB (absent from source)")
                    conn.execute(text("DELETE FROM sports_activities WHERE id = :id"), {"id": row['id']})
                    details.append(f"{ct.WARNING} : deleted activity :")
                    details.append(f"{ct.WARNING} : Old values : {params_log}")
                    status = ct.WARNING
                    nb_del +=1
            # Si on arrive ici, SQLAlchemy fait le COMMIT automatiquement
            details.append(f"Number of saved new activities : {nb_mod}")
            details.append(f"Number of saved modified activities : {nb_mod}")
            details.append(f"Number of saved deleted activities : {nb_del}")
            if status == ct.WARNING:
                logger.warning("Sauvegarde réussie avec anomalie : COMMIT effectué.")
            else:
                logger.info("Sauvegarde réussie : COMMIT effectué.")
            shape = df_todo.shape
    except Exception as e:
        logger.error(f"Error during loading {processed_file} to postgreSQL: {e}")
        status = ct.FAILED
    logger.info(f"End incoming comments generation to postgreSQL")
    ct.kestra_output("detail", details,sep=f"{ct.SP2}- ",lf=True )
    ct.kestra_output("shape", shape, sep= " X ", trail= False)
    ct.kestra_output("result", ct.STATUS_TXT[status])
    ct.kestra_output("status", status, raw=True)
    sys.exit(ct.make_exit_status(status))


if __name__ == "__main__":
    import sys
    SIMPLE_COMMENTS = True
    action = sys.argv[1]
    # CLI parameters
    BASE_DIR = Path(__file__).parent.parent.parent
    ct.KESTRA_MODE = False
    if action == "rung_gen":
        run_generation(f"{BASE_DIR}/data/sources/Données+Sportive.xlsx", f"{BASE_DIR}/data/tmp/activities_fake.xlsx", f"{BASE_DIR}/data/sources/strava_sports.xlsx", f"{BASE_DIR}/data/sources/locations.xlsx", 3000)
    elif action == "validate":
        validate_incoming(f"{BASE_DIR}/data/tmp/activities_fake.xlsx", f"{BASE_DIR}/data/tmp/activities_income.parquet")
    elif action == "fingerprint":
        fingerprint_activities(f"{BASE_DIR}/data/tmp/activities_income.parquet",f"{BASE_DIR}/data/tmp/activities_fingerprint.parquet", )
    elif action == "scan":
        scan_new_activities( f"{BASE_DIR}/data/tmp/activities_fingerprint.parquet",f"{BASE_DIR}/data/tmp/activities_actions.parquet")
    elif action == "comment":
        comment_activities(f"{BASE_DIR}/data/tmp/activities_actions.parquet",f"{BASE_DIR}/data/tmp/activities_commented.parquet", f"{BASE_DIR}/data/sources/strava_sports.xlsx")  
    elif action == "load_pg":
        load_pg(f"{BASE_DIR}/data/tmp/activities_commented.parquet")
