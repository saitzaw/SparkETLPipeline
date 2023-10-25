# Install Apache Airflow 
- Install apache airflow in VM using the python virtual envrionment
- Create a daemon to run Airflow webserver 
- Create a daemon to run Airflow scheduler
- Change the configuration in the airflow.cfg 

# Install Apache Spark
- Install Aapche Spark in VM which has the Airflow Server
- Extract tarball file in /opt/ and change the name to /opt/spark/
- Config the Spark environment 
- Run the Server
- Shell script to run the Apache Spark
- export SPARK_HOME="/opt/spark"


# Dags 
- Create a daily dag file to run the Airflow server
- Cerate a ETL pipeline python script to run pipeline

# How to run the mini data infra
## Run Airflow server
- sudo systemctl start airflow-webserver.service
- sudo systemctl start airflow-scheduler.service
- Check in web browser localhost:8080

## Run Spark server
- cd /opt/spark
- Run run.sh 
- Change the default port to 7777
- Check in web browser localhost:7777
 
