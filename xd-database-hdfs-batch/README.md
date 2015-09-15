Spring XD Batch Demo
====================

This is the XD version of the `database-to-hdfs-batch` demo.

## Requirements

In order for the sample to run you will need to have installed:

* Spring XD 1.2.1.RELEASE or later
* MySQL
* Hadoop

Configure Spring XD to use MySQL

### Edit `conf/servers.yml`

```
spring:
  datasource:
    url: jdbc:mysql://yourDBhost:3306/batch_demo
    username: root
    password: root
    driverClassName: com.mysql.jdbc.Driver
    validationQuery: select 1
```

Copy MySQL Driver to the Spring XD `/lib/` directory.

### For easy SMTP testing - FakeSMTP

https://nilhcem.github.io/FakeSMTP/

## Build+Run the Demo

### Build the Spring XD module

First, verify the settings in `src/main/resources/application.properties`

In `/xd-database-hdfs-batch/` execute:

	$ mvn clean package

### Start Spring XD

Start your *Spring XD* single node server:

	xd/bin>$ ./xd-singlenode

### Start the *Spring XD Shell* in a separate window:

	shell/bin>$ ./xd-shell

### Install the job

	xd:>module upload --type job --name mysql2hdfsjob --file [path-to]/xd-database-hdfs-batch/target/xd-database-hdfs-batch-1.0.0.BUILD-SNAPSHOT.jar

### Run the following command in the XD Shell

	xd:> job create mysql2hdfs --definition "mysql2hdfsjob" --deploy

	xd:> stream create inhttp --definition "http > queue:job:mysql2hdfs" --deploy

	xd:> stream create joblog --definition "tap:job:mysql2hdfs.job > log" --deploy

	xd:> stream create job-notifications --definition "tap:job:mysql2hdfs.job > transform --expression='\"Job Finished with Execution ID\" + payload.id' | mail --to='\"springone2gx@roc.ks\"' --host=localhost --subject='\"Job Complete\"'" --deploy

Execute the job by posting to the created HTTP endpoint:

	xd:> http post --data {}


