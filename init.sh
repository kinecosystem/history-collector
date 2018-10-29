#!/usr/bin/env bash
set -e

echo Taking existing containers down
sudo docker-compose down -v

echo Removing previous data
sudo rm -rf grafana/grafana-data postgres/postgres-data

echo Settning up database container
sudo docker-compose up -d db
sleep 8

echo Setting up ETL container
sudo docker-compose up -d ETL
sleep 3

echo Setting up grafana container
sudo docker-compose up -d grafana
