import pandas as pd
import os, time
import logging
from sqlalchemy import create_engine
from faker import Faker
import great_expectations as gx
from datetime import datetime, timedelta
from pathlib import Path

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
fake = Faker('fr_FR')
MAX_ROWS = None




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


def simulate_and_ingest_sports(num_activities=1000):
    """
    Génération de données sportives Faker et validation GX.
    """
    logger.info("Démarrage de la simulation sportive...")
    
    # Récupération des employés valides pour l'intégrité référentielle
    employees = pd.read_sql("SELECT \"ID salarié\" FROM employees", engine)
    emp_ids = list(employees)
    
    activities = []
    for _ in range(num_activities):
        start = fake.date_time_between(start_date='-1y', end_date='now')
        duration = fake.random_int(min=300, max=10800)
        end = start + timedelta(seconds=duration)
        
        activities.append({
            'activity_id': fake.uuid4(),
            'id_salarie': fake.random_element(elements=emp_ids),
            'timestamp_start': start,
            'timestamp_end': end,
            'sport_type': fake.random_element(elements=('Course à pied', 'Vélo', 'Marche', 'Randonnée', 'Escalade')),
            'distance_m': fake.random_int(min=0, max=30000),
            'comment': fake.sentence()
        })
    
    df_sports = pd.DataFrame(activities)
    
    # Validation GX sur la simulation
    sports_expectations = {
        gx.expectations.ExpectColumnValuesToNotBeNull: [
            {"column": "id_salarie"},
            {"column": "timestamp_start"}
        ],
        gx.expectations.ExpectColumnValuesToBeBetween: [
            {"column": "distance_m", "min_value": 0}
        ]
    }
    
    results = validate_dataframe(df_sports, "sports_sim_suite", sports_expectations)
    
    if results.success:
        # Insertion dans la table suivie par Debezium
        df_sports.to_sql('sports_activities', engine, if_exists='replace', index=False)
        logger.info("Simulation sportive validée et insérée.")
    else:
        logger.error("Erreur de qualité dans la simulation.")
        raise ValueError("La simulation a produit des données non conformes.")

if __name__ == "__main__":
    # Ce bloc sera piloté par Kestra en passant les chemins de fichiers nécessaires
    import sys
    action = sys.argv[1]
    if action == "hr":
        MAX_ROWS = 5
        BASE_DIR = Path(__file__).parent.parent.parent
        #ingest_hr_data(f"{BASE_DIR}/data/sources/Données+RH.xlsx")
    elif action == "sports":
        simulate_and_ingest_sports()