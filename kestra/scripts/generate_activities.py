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

fake = Faker('fr_FR')

ACT_VS_INACT = 2


def run_generation(
    source_xlsx: str, 
    output_parquet: str, 
    excel_sport_file: str, 
    num_rows: int = 1200
) -> None:
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
    sport_engine = ct.Sport_engine(excel_sport_file, ct.START_DATE)
 
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
    while nb_created <= num_rows:
    
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
        cur_activity['id'] = str(uuid.uuid1())
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
            new_activity['distance_meters'] = perf['distance_meters']
            new_activity['begin_date'] = perf['begin_date']
            new_activity['end_date'] = perf['end_date']
            final_activities.append(new_activity)
            nb_created +=1

    # 3. Data persistence and Kestra output
    df_result = pd.DataFrame(final_activities)
    df_result.to_parquet(output_parquet, index=False)
    
    # Integration with Kestra context
    ct.kestra_output('activities_path',  output_parquet)

if __name__ == "__main__":
    # CLI parameters
    BASE_DIR = Path(__file__).parent.parent.parent

    run_generation(f"{BASE_DIR}/data/sources/Données+Sportive.xlsx", f"{BASE_DIR}/kestra/tmp/activities.parquet", f"{BASE_DIR}/data/sources/strava_sports.xlsx", 500)