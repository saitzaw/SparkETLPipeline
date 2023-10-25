# Airflow server dependencies 
- pip install apache-airflow==2.6.1
- pip install pandas 
- pip install pymysql
- pip install psycopg2-binary
- pip install apache-airflow-providers-postgres

### start the Database 
- airflow db init


### Run Apache Airflow 
- airflow webserver -p 8080

### Install local database for Apache Airflow 
- sudo apt install postgresql postgresql-contrib

### start the postgresql service  
- sudo systemctl start postgresql.service

### create a user name and db for airflow 
- postgres=# CREATE DATABASE airflow;
- postgres=# CREATE USER airflow WITH ENCRYPTED PASSWORD 'AirFlow';
- postgres=# GRANT ALL PRIVILEGES ON DATABASE airflow to airflow;
- postgres=# ALTER DATABASE airflow OWNER TO airflow ; #Change owner

### change the postgresql config
sudo vi /etc/postgresql/10/main/pg_hba.conf
### carefully change this 

###### IPv4 local connections:
###### host    all             all             0.0.0.0/0            trust
host all all all trust
 

### Change the airflow config file 
- vim ~/airflow/airflow.cfg

### change executoer 
executor = LocalExecutor

### change database connection
sql_alchemy_conn = postgresql+psycopg2://airflow:AirFlow@localhost:5432/airflow

# Create an Airflow Service files 
### service file path 
- /etc/system/systemd
### Daemons 
- create: sudo vim airflow-webserver.service
- create: sudo vim airflow-scheduler.service 

# Apache Spark
### spark-run 
sudo cp spark-run.sh /opt/spark
sudo cp spark-stop.sh /opt/spark

# Remark change the $HOME in service file for production server


