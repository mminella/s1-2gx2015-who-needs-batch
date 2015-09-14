Flink Demo
==========

Welcome to the [Apache Flink][] Demo! This application uses Flink's DataSet API to
retrieve [StackOverflow][] from an MySql database and writes that data to an Hadoop
Distributed File System (HDFS).

## Requirements

* Java 7+
* MySQL database
* Hadoop
* Import the test data to Mysql. See the `database-import` module for details.

### Hadoop

Make sure Hadoop is up an running. For a local setup, you can follow the installation
procedure as outlined in the Spring XD reference guide:

http://docs.spring.io/spring-xd/docs/1.2.1.RELEASE/reference/html/#_installing_hadoop

### Flink

The easiest option is to import the demo project into your favorite IDE, e.g. Spring Tool Suite and run
the main class `io.spring.flink.Jdbc2HdfsSample`. No special Flink setup is necessary
in that situation as Flink is bootstrapped automatically for you.

Next you can the demo using a proper Flink installation. Please following the setup
steps outlined here:

https://ci.apache.org/projects/flink/flink-docs-release-0.9/quickstart/setup_quickstart.html

To make sure that Flink is running, access the Flink dashboard at:

http://localhost:8081/index.html

## Running the Demo

Go to your Flink directory and execute:

	$ bin/flink run /path/to/flink-demo/target/flink-demo-1.0-SNAPSHOT.jar

The demo should execute successfully and **6 CSV files** should show up in HDFS.

### Possible Issues

In case you see the following error:

```
java.lang.OutOfMemoryError: GC overhead limit exceeded
	at com.mysql.jdbc.MysqlIO.reuseAndReadPacket(MysqlIO.java:3387)
	at com.mysql.jdbc.MysqlIO.reuseAndReadPacket(MysqlIO.java:3334)
	at com.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:3774)
	at com.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:871)
	at com.mysql.jdbc.MysqlIO.nextRow(MysqlIO.java:1940)
	at com.mysql.jdbc.MysqlIO.readSingleRowSet(MysqlIO.java:3285)
	at com.mysql.jdbc.MysqlIO.getResultSet(MysqlIO.java:463)
	at com.mysql.jdbc.MysqlIO.readResultsForQueryOrUpdate(MysqlIO.java:3009)
	at com.mysql.jdbc.MysqlIO.readAllResults(MysqlIO.java:2257)
	at com.mysql.jdbc.MysqlIO.sqlQueryDirect(MysqlIO.java:2650)
	at com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2541)
	at com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2499)
	at com.mysql.jdbc.StatementImpl.executeQuery(StatementImpl.java:1432)
	at org.apache.flink.api.java.io.jdbc.JDBCInputFormat.open(JDBCInputFormat.java:86)
	at org.apache.flink.runtime.operators.DataSourceTask.invoke(DataSourceTask.java:151)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:559)
	at java.lang.Thread.run(Thread.java:745)
```

You may have to increase the memory settings for Flink. In your Flink directory,
open `conf/flink-conf.yaml` and increase the settings. E.g.:

```
jobmanager.heap.mb: 512
taskmanager.heap.mb: 2048
```

[Apache Flink]: https://flink.apache.org/
[StackOverflow]: http://stackoverflow.com/