import pandas as pd
import numpy as np
import random
from faker import Faker
import sys
from kestra import Kestra
from pathlib import Path
import common_tools as ct
import fastparquet
import uuid

fake = Faker('fr_FR')

ACT_VS_INACT = 2


def run_generation(source_xlsx, output_parquet, excel_sport_file, num_rows=1200):
    # 1. Charger le référentiel des employés
    df_ref = pd.read_excel(source_xlsx, names= ct.SPORT_COLUMNS, dtype= ct.SPORT_MAPPING,header=0)
    
    mask_active = df_ref['sport_type'].notna()
    df_actifs = df_ref[mask_active]
    df_inactifs = df_ref[~mask_active]

    # 2. Liste des sports pour l'aléatoire (cohérent avec ton code précédent)
    sport_engine = ct.Sport_engine(excel_sport_file, ct.START_DATE)
 
    activities = []
    
    nb_active = len(df_actifs)
    poids_actifs_bruts = 1 / np.power(np.arange(1, nb_active + 1), 0.4)
    np.random.shuffle(poids_actifs_bruts)
    poids_actifs_norm = poids_actifs_bruts / poids_actifs_bruts.sum()

    nb_inactive = len(df_inactifs)
    # --- Pour les INACTIFS (Exponentielle scale 70) ---
    poids_inactifs_bruts = np.random.exponential(scale=70, size=nb_inactive)
    poids_inactifs_norm = poids_inactifs_bruts / poids_inactifs_bruts.sum()

    nb_created = 0
    final_activities = []

    while nb_created <= num_rows:
    
        # Sélection d'un employé au hasard
        
        ratio = (ACT_VS_INACT * nb_active) / (ACT_VS_INACT * nb_active + nb_inactive)
        if random.random() < ratio and not df_actifs.empty:
            # Choix parmi les sportifs avec distribution Puissance
            emp = random.choices(
                population=df_actifs.index.tolist(),
                weights=poids_actifs_norm,
                k=1
            )[0]
            selected_emp_row = df_actifs.loc[emp]
        else:
            # Choix parmi les inactifs avec distribution Exponentielle
            emp = random.choices(
                population=df_inactifs.index.tolist(),
                weights=poids_inactifs_norm,
                k=1
            )[0]
            selected_emp_row = df_inactifs.loc[emp]

        fav_sport = selected_emp_row['sport_type']
        cur_activity={}
        cur_activity['id'] = str(uuid.uuid1())
        cur_activity['employee_id'] = selected_emp_row['id']

        # Logique de probabilité (85% sport préféré)
        if random.random() < 0.85 and isinstance(fav_sport, str) :
            activity = sport_engine.get_random_activity_by_name(fav_sport)
        else:
            activity = sport_engine.get_random_activity()
        
        cur_activity['sport'] = activity['sport'] 
        for perf in activity['perfs']:
            cur_activity['distance_meters'] = perf['distance_meters']
            cur_activity['begin_date'] = perf['begin_date']
            cur_activity['end_date'] = perf['end_date']
            final_activities.append(cur_activity)
            nb_created +=1

    # 3. Sauvegarde en CSV pour le plugin Kestra
    df_result = pd.DataFrame(final_activities)
    df_result.to_parquet(output_parquet, index=False)
    
    # On informe Kestra du fichier généré
    ct.kestra_output('activities_path',  output_parquet)

if __name__ == "__main__":
    # Paramètres passés par Kestra
    BASE_DIR = Path(__file__).parent.parent.parent

    run_generation(f"{BASE_DIR}/data/sources/Données+Sportive.xlsx", f"{BASE_DIR}/kestra/tmp/activities.parquet", f"{BASE_DIR}/data/sources/strava_sports.xlsx", 500)