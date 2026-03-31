#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -U "$POSTGRES_USER"; do
  echo "PostgreSQL unavailable - sleeping"
  sleep 1
done

# Create databases from sport_app_db (if not exists)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
  SELECT 'CREATE DATABASE sport_app_db' 
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sport_app_db')\gexec
  SELECT 'CREATE DATABASE kestra' 
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kestra')\gexec
EOSQL

# Create RH employees table
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "sport_app_db" <<-'EOSQL'
  CREATE TABLE IF NOT EXISTS employees (
    id VARCHAR(50) PRIMARY KEY,
    last_name VARCHAR(100),
    first_name VARCHAR(100),
    age INTEGER,
    business_unit VARCHAR(50),
    seniority_years  INTEGER, 
    vacation_days INTEGER,
    employement_contract VARCHAR(20),
    salary DECIMAL(10,2),
    transport_mode VARCHAR(50),
    distance_kms DECIMAL(10,1),
    margin_kms DECIMAL(10,1),
    sport_type VARCHAR(50)
  );
EOSQL

# Create sports activities table (no cross-DB FK constraint)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "sport_app_db" <<-'EOSQL'
  CREATE TABLE IF NOT EXISTS public.sports_activities (
    id text NOT NULL,
    employee_id text NULL,
    sport varchar(50) NULL,
    situation varchar(50) NULL,
    distance_meters int4 NULL,
    begin_date timestamp NULL,
    duration_sec int4 NULL,
    comment text NULL,
    fingerprint text NULL,
    CONSTRAINT sports_activities_pkey PRIMARY KEY (id),
    CONSTRAINT sports_activities_employees_fk FOREIGN KEY (employee_id) REFERENCES public.employees(id) ON DELETE RESTRICT
  );
  
EOSQL

echo "✅ Databases and tables initialized successfully"

