package io.spring.batch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.fs.FileSystemFactoryBean;

@SpringBootApplication
public class DatabaseToHdfsApplication {

    @Autowired
    public org.apache.hadoop.conf.Configuration configuration;

    @Bean
    public FileSystemFactoryBean fileSystem() {
        FileSystemFactoryBean fileSystemFactoryBean = new FileSystemFactoryBean();

        fileSystemFactoryBean.setConfiguration(configuration);

        return fileSystemFactoryBean;
    }

    public static void main(String[] args) {
        SpringApplication.run(DatabaseToHdfsApplication.class, args);
    }
}
