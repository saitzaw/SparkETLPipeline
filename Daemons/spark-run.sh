#!/bin/bash
./sbin/start-master.sh
./sbin/start-worker.sh spark://pop-os.localdomain:7077

if [ ! -d /tmp/spark-events ]; then
	mkdir /tmp/spark-events
fi

chmod 777 -R /tmp/spark-events
./sbin/start-history-server.sh
