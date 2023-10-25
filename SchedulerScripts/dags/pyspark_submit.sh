cd /opt/spark
./bin/spark-submit --master spark://pop-os.localdomain:7077 --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar --driver-memory 8g --executor-memory 12g --properties-file /opt/spark/conf/spark-config.conf /opt/spark/python/pyspark_etl.py 
