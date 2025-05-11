# Real-Time Log Processing Pipeline Using Apache Big Data Tools
Starting HDFS and YARN using the following commands:
start-dfs.sh
start-yarn.sh
______________________________
Starting the log generator code by running the python file or by jupyter notebook
______________________________
Starting the kafka server and zookeeper and creating a topic and listener to test the connectivity using the following commands:
cd $KAFKA_HOME

bin/zookeeper-server-start.sh config/zookeeper.properties 

bin/kafka-server-start.sh config/server.properties 

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic pykafka

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pykafka --from-beginning
__________________________________________________________________
Create flume agent configration file and run the agent to move data from log generator to kafka using the following commands: 

nano /home/bigdata/apache-flume-1.7.0-bin/conf/python-to-kafka.conf -> past the configration on the attached file.
#runnig the flume agent
cd /home/bigdata/apache-flume-1.7.0-bin
bin/flume-ng agent \--conf conf \--conf-file conf/python-to-kafka.conf \--name agent \-Dflume.root.logger=INFO,console
________________________________________________________
Data is now on kafka and it should be displayed on the listener
_________________________________________________________
Create flume agent configration file and run the agent to move data from kafka to HDFS using the following commands: 

nano /home/bigdata/apache-flume-1.7.0-bin/conf/pytho-to-kafka-to-hdfs.conf-> past the configration on the attached file.
#running the flume agent
cd /home/bigdata/apache-flume-1.7.0-bin
bin/flume-ng agent \--conf conf \--conf-file conf/pytho-to-kafka-to-hdfs.conf \--name agent \-Dflume.root.logger=INFO,console
______________________________________________
Data should now be displayed on HDFS
____________________________________________
Run the pyspark script that takes the data from kafka and analys it using the attached file (kafka-spark)
