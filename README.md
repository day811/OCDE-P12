
---

# 🏃‍♂️ SDS Sport Pipeline - Monitoring d'Activité & AI Coaching

**Projet OpenClassrooms P12** - Pipeline Data End-to-End : Ingestion, Change Data Capture (CDC), Streaming, Enrichissement par IA et Analytics.

---

## 📋 Table des matières

- [Présentation](#présentation)
- [Objectifs](#objectifs)
- [Architecture](#architecture)
- [Prérequis](#prérequis)
- [Installation Simplifiée](#installation-simplifiée)
- [Structure du projet](#structure-du-projet)
- [Pipeline de données](#pipeline-de-données)
- [Technologies](#technologies)
- [Qualité des données](#qualité-des-données)
- [Visualisation Power BI](#visualisation-power-bi)
- [Configuration](#configuration)

---

## 🎯 Présentation

**SDS Sport Pipeline** est un système complet de suivi de l'activité physique des employés au sein de l'entreprise SDS. Le projet automatise la collecte de données depuis un Google Sheet, traite les flux en temps réel via une architecture "Medallion" et utilise l'IA générative pour motiver les employés sur Slack.

Le système intègre des technologies de pointe telles que **Kestra** pour l'orchestration, **Debezium/Redpanda** pour le streaming d'événements, et **Spark Structured Streaming** pour le traitement distribué.

---

## 🎯 Objectifs

1. ✅ **CDC (Change Data Capture)** : Capture en temps réel des modifications de la base de données via Debezium.
2. ✅ **Traitement de Flux** : Transformation des données "on-the-fly" avec Spark Streaming vers le format Delta Lake.
3. ✅ **IA Générative** : Utilisation de Google Gemini pour transformer des métriques brutes en commentaires de coaching humains et motivants.
4. ✅ **Sécurité** : Chiffrement des données sensibles (noms, adresses) avec Fernet.
5. ✅ **Qualité des données** : Validation automatisée des schémas et des règles métier avec Great Expectations.
6. ✅ **Dashboarding** : Visualisation dynamique des statistiques et simulation de bonus dans Power BI.

---

## 🏗️ Architecture

### Flux de données global

```
┌─────────────────┐
│  Google Sheets  │  ← Saisie des activités par les employés
└────────┬────────┘
         │ (Kestra: ingest_activities) 
         ▼
┌─────────────────┐
│   PostgreSQL    │  ← Base transactionnelle (Staging) 
└────────┬────────┘
         │ (Debezium Connector) 
         ▼
┌─────────────────┐
│    Redpanda     │  ← Bus d'événements (Kafka) 
└────────┬────────┘
         │ (Spark Structured Streaming) 
         ▼
┌─────────────────┐      ┌─────────────────┐
│   Gemini AI     │ ↔️   │  Spark Processing│
└─────────────────┘      └────────┬────────┘
                                  │
                                  ▼
┌─────────────────┐      ┌─────────────────┐
│  Slack (Notif)  │ ←──  │ Delta Lake (S3) │  ← Stockage analytique
└─────────────────┘      └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │ Power BI Dashboard
                         └─────────────────┘
```

---

## 📦 Prérequis

- **Docker & Docker Compose**
- **Clé API Google Gemini** (Google AI Studio)
- **Compte Service Google Cloud** (pour l'accès au Google Sheet)
- **Webhook Slack** (pour les notifications de coaching)

---

## 🚀 Installation Simplifiée

### 1. Déploiement de l'infrastructure
Clonez le dépôt et lancez les conteneurs :
```bash
git clone <votre-repo>
cd OCDE-P12
docker-compose up -d
```

### 2. Initialisation des Flux (Kestra)
Pour simplifier le déploiement, l'importation de l'environnement est automatisée :
1.  Accédez à l'interface Kestra : `http://localhost:8080`.
2.  Créez un nouveau flux.
3.  Copiez-collez le contenu du fichier `kestra/flows/git_sync.yml`.
4.  **Exécutez ce flux.** * *Action :* Ce flux synchronise tous les autres flows, importe les scripts Python dans le stockage Kestra et configure les connecteurs Debezium.

---

## 📁 Structure du projet

| Composant | Fichier / Dossier | Rôle |
|-----------|-------------------|------|
| **Orchestration** | `kestra/flows/` | Définition des workflows YAML (Main, Ingestion, Streams). |
| **Ingestion RH** | `ingest_hr.py` | Import, chiffrement et géocodage des employés. |
| **Worker Activités**| `activity_worker.py` | Simulation de données et détection des nouvelles activités. |
| **Stream Slack** | `process_activities_slack.py` | Spark : Récupère les données, appelle l'IA et envoie vers Slack. |
| **Stream Stats** | `process_activities_stats.py` | Spark : Calcule les années sociales et agrège pour le BI. |
| **Engine RAG/IA** | `rag/engine.py` | Logique d'appel multi-LLM (Gemini/Mistral). |
| **Common Tools** | `common_tools.py` | Fonctions partagées (Encryption, GX Validation). |

---

## ⚙️ Pipeline de données

### 1. Ingestion (Bronze Layer)
Le script `ingest_hr.py` récupère les données Excel/GSheet, applique un chiffrement Fernet sur les PII (Données d'Identification Personnelle) et calcule les distances domicile-travail via l'API Google Maps.

### 2. Capture et Streaming (Silver Layer)
* **CDC** : Debezium surveille PostgreSQL et pousse chaque changement dans Redpanda.
* **Spark Processing** : Les jobs Spark consomment les topics Kafka. 
    * `process_employees` détermine l'éligibilité aux bonus.
    * `process_activities_stats` formate les dates pour l'analyse temporelle.

### 3. IA Enrichment & Slack (Gold Layer)
Le script `process_activities_slack.py` intercepte les nouvelles activités. Il envoie un prompt enrichi au moteur RAG :
* **Prompt Engineering** : Le coach reçoit l'ID, le nom, le sport et la performance.
* **Génération** : Gemini 2.5 Flash génère un commentaire motivant en format JSON.
* **Action** : Le commentaire est envoyé à un webhook Kestra qui poste sur Slack.

---

## 🤖 Technologies

| Couche | Technologie | Rôle |
|--------|-------------|------|
| **Orchestrateur** | Kestra | Pilotage des tâches et gestion des secrets. |
| **Streaming** | Redpanda | Broker de messages compatible Kafka. |
| **Moteur Data** | Apache Spark | Traitement de flux distribué. |
| **IA** | Google Gemini | Génération de commentaires personnalisés. |
| **Data Lake** | Delta Lake | Stockage ACID pour le reporting. |
| **Validation** | Great Expectations | Tests de qualité de données automatisés. |
| **Base de données** | PostgreSQL | Stockage transactionnel et metadata. |

---

## 🧪 Qualité des données

Le projet utilise **Great Expectations (GX)** pour garantir l'intégrité du pipeline.
* **RH** : Vérification de l'unicité des IDs, des tranches d'âge (16-80 ans) et de la cohérence des contrats.
* **Activités** : Validation de l'existence de l'employé en base avant l'insertion de l'activité.
* **WAP Pattern** : Les données ne sont publiées (Publish) que si l'audit (Audit) est validé.

---

## 📊 Visualisation Power BI

Le dashboard se connecte aux fichiers **Delta Lake** via DuckDB.
* **Simulation de Prime** : Un paramètre "What-if" dans Power BI permet d'ajuster dynamiquement le taux de bonus (ex: 5%) sur le salaire des employés éligibles.
* **Fiche Employé** : Vue détaillée avec les commentaires générés par l'IA et les performances sportives agrégées par année sociale.

---

## ⚙️ Configuration (.env)

```bash
# API Keys
GEMINI_API_KEY=your_gemini_key
GOOGLE_MAPS_KEY=your_maps_key
MISTRAL_API_KEY=your_mistral_key

# Database
POSTGRES_USER=sds_admin
POSTGRES_PASSWORD=your_password
DB_CONNECTION_STRING=postgresql://sds_admin:your_password@postgres:5432/sport_app_db

# Security
CRYPT_KEY=your_fernet_key # Clé de chiffrement 32 bytes

# Logic
FIRST_MONTH=9 # Début de l'année sociale (Septembre)
```

---

**👤 Projet réalisé par Yves Dangel**
Formation Data Engineer - 2026.