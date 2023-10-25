cd /opt/spark
#./bin/spark-submit --master spark://pop-os.localdomain:7077 --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar --driver-memory 8g --executor-memory 12g /home/sthz/PersonalProject/MySpark/Scripts/pyspark_etl.py 
./bin/spark-submit --master spark://pop-os.localdomain:7077 --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar --driver-memory 8g --executor-memory 12g --properties-file /opt/spark/conf/spark-config.conf /home/sthz/PersonalProject/MySpark/Scripts/pyspark_etl.py 
