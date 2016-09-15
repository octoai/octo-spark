### Build

```bash
$ sbt clean package
```


### Execute

```bash
$ $SPARK_HOME/bin/spark-submit --class org.octo.HelloWorldExample --jars ~/etc/spark-cassandra-connector-assembly-2.0.0-M3.jar target/scala-2.11/octo-spark_2.11-1.0.jar
```

