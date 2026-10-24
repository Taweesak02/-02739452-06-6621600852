#!/bin/bash
set -e

echo "Waiting for Airflow database..."

until airflow db check; do
    sleep 3
done

echo "Migrating Airflow database..."
airflow db migrate

echo "Creating Airflow admin user..."

airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    || echo "Admin user already exists"

echo "Starting scheduler..."
airflow scheduler &

echo "Starting webserver..."
exec airflow webserver