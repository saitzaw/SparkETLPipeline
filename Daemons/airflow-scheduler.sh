#!/bin/bash 
PROJECT_NAME="AIRFLOW"
PROJECT_PATH=$HOME
PROJECT_ENV=$PROJECT_PATH/.airflow-env/bin/activate

if [ -d $PROJECT_PATH ]; then
        cd $PROJECT_PATH
        source $PROJECT_ENV
        export AIRFLOW_HOME=$PROJECT_PATH/airflow
        airflow scheduler
        deactivate
fi
