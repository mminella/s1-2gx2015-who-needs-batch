package io.spring.batch.workflow;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.fs.FileSystemFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
@EnableBatchProcessing
public class WorkflowApplication extends DefaultBatchConfigurer {

    @Autowired
    public org.apache.hadoop.conf.Configuration configuration;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public PlatformTransactionManager transactionManager;

	@Override
	protected JobRepository createJobRepository() throws Exception {
		JobRepositoryFactoryBean factoryBean = new JobRepositoryFactoryBean();

		factoryBean.setDataSource(dataSource);
		factoryBean.setIsolationLevelForCreate("ISOLATION_REPEATABLE_READ");
		factoryBean.setTransactionManager(transactionManager);

		factoryBean.afterPropertiesSet();

		return factoryBean.getObject();
	}

    @Bean
    public FileSystemFactoryBean fileSystem() {
        FileSystemFactoryBean fileSystemFactoryBean = new FileSystemFactoryBean();

        fileSystemFactoryBean.setConfiguration(configuration);

        return fileSystemFactoryBean;
    }

    public static void main(String[] args) {
        SpringApplication.run(WorkflowApplication.class, args);
    }
}
