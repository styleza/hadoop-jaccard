javac Task3.java -cp $(hadoop classpath) -Xlint:deprecation
jar cf t3.jar Task3*.class
hadoop jar t3.jar Task3 /input/s1.txt /input/s2.txt /output,
hadoop fs -cat /output/part-m-00000