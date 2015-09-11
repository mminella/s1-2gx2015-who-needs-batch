## Workflow example
This demo is based on the Oozie example found here: http://www.infoq.com/articles/oozieexample

To run this job, you'll need to create an application.properties with the following values:

```
spring.datasource.driverClassName=com.mysql.jdbc.Driver
spring.datasource.url=<URL_FOR_YOUR_DATABASE>
spring.datasource.username=<USERNAME_FOR_YOUR_DATABASE>
spring.datasource.password=<PASSWORD_FOR_YOUR_DATABASE>
spring.datasource.schema=schema-mysql.sql
spring.hadoop.fsUri: <URL_FOR_YOUR_HDFS>
spring.mail.host=<YOUR_SMTP_HOST>
spring.mail.port=<YOUR_SMTP_PORT>
spring.mail.username=<YOUR_SMTP_USERNAME>
spring.mail.password=<YOUR_SMTP_PASSWORD
spring.mail.properties.mail.smtp.auth=<BOOLEAN_IF_AUTH_IS_REQIRED>
spring.mail.properties.mail.smtp.starttls.enable=<BOOLEAN_IF_TLS_IS_ENABLED>
spring.batch.job.names=mainJob
job.email.to=<EMAIL_TO_SEND_EMAIL_TO>
```
