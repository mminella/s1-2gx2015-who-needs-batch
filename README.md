Who Needs Batch
============

This project contains the demo code for the presentation **Who needs batch these days?**:

* https://2015.event.springone2gx.com/schedule/sessions/who_needs_batch_these_days.html

# Requirements

* Java 8
* Maven

# StackOverflow data

The demos will use StackOverflow data that initially is imported to a relational database (MySql).

https://archive.org/details/stackexchange

## Import of StackOverflow data

Before running the website or the job, you'll need to download and import the data.  The data is provided via XML files that will be imported via a [Spring Batch](http://spring.io/projects/spring-batch) job.

1. Build the project by running `$ mvn package` from the root of the repository (same place this file is located).
2. Execute the job via the command `$ java -jar database-import/target/database-import-1.0-SNAPSHOT.jar importDirectory=<IMPORT_LOCATION>`.  <IMPORT_LOCATION> is the location of the StackOverflow XML files.  This directory should contain at least the Votes.xml, Users.xml, Posts.xml, PostHistory.xml, and Comments.xml (no other data is used for this demo).  This will create the database tables used by the website as well as import the XML data into them.
3. With the data imported, you can either run the website and take a look around or begin processing the data for the recommendation engine.

**Troubleshooting**

*Unable to connect to the database* - Update the values in the application.properties file located in database-import/src/main/resources to be correct for your database instance.
