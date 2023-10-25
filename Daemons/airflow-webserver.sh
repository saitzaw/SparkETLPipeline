#!/bin/bash 
PROJECT_NAME="AIRFLOW"
PROJECT_PATH=$(dirname $0)
PROJECT_ENV=$PROJECT_PATH/.airflow-env/bin/activate

if [ -d $PROJECT_PATH ]; then
        cd $PROJECT_PATH
        source $PROJECT_ENV
        export AIRFLOW_HOME=$PROJECT_PATH/airflow
        airflow webserver -p 8080 --pid /run/airflow/webserver.pid
        deactivate
fi
