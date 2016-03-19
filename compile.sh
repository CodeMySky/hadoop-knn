javac -classpath /usr/hdp/current/hadoop-client/client/hadoop-common.jar:/usr/hdp/current/hadoop-client/client/commons-cli.jar:/usr/hdp/current/hadoop-client/client/hadoop-mapreduce-client-core.jar *.java
jar cvf KNN.jar *.class
rm *.class
hdfs dfs -rm -r output
hadoop jar KNN.jar KNN test.csv output
