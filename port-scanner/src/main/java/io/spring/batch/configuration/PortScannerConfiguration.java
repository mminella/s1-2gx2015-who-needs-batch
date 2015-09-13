/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.configuration;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import io.spring.batch.domain.Target;
import io.spring.batch.domain.TargetRowMapper;
import io.spring.batch.partition.ColumnRangePartitioner;
import io.spring.batch.processor.TargetScanItemProcessor;
import io.spring.batch.tasklet.LoadPortsTasklet;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * @author Michael Minella
 */
@Configuration
public class PortScannerConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	@StepScope
	public LoadPortsTasklet loadPortsTasklet(DataSource dataSource,
			@Value("#{jobParameters['ipAddress']}") String ipAddress) {
		LoadPortsTasklet tasklet = new LoadPortsTasklet();

		tasklet.setDataSource(dataSource);
		tasklet.setIpAddress(ipAddress);
		tasklet.setNumberOfPorts(1024);

		return tasklet;
	}

	@Bean
	@StepScope
	public Partitioner partitioner(DataSource dataSource) {
		ColumnRangePartitioner partitioner = new ColumnRangePartitioner();

		partitioner.setDataSource(dataSource);
		partitioner.setColumn("id");
		partitioner.setTable("target");

		return partitioner;
	}

	@Bean
	public JdbcPagingItemReader<Target> exportItemReader(DataSource dataSource) {
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("port", Order.ASCENDING);

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();

		queryProvider.setSelectClause("ID, IP, PORT, CONNECTED, BANNER");
		queryProvider.setFromClause("FROM TARGET");
		queryProvider.setWhereClause("CONNECTED IS TRUE");
		queryProvider.setSortKeys(sortKeys);

		JdbcPagingItemReader<Target> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setQueryProvider(queryProvider);
		reader.setPageSize(100);
		reader.setRowMapper(new TargetRowMapper());

		return reader;
	}

	@Bean
	@StepScope
	public StaxEventItemWriter<Target> xmlOutputWriter(Marshaller marshaller,
			@Value("#{jobParameters['outputFile']}") String outputFile) throws Exception {
		StaxEventItemWriter<Target> writer = new StaxEventItemWriter<>();

		writer.setResource(new FileSystemResource(outputFile));
		writer.setMarshaller(marshaller);
		writer.setRootTagName("openTargets");

		writer.afterPropertiesSet();

		return writer;
	}

	@Bean
	public XStreamMarshaller marshaller() {
		Map<String, Class> aliases = new HashMap<>();
		aliases.put("target", Target.class);

		XStreamMarshaller marshaller = new XStreamMarshaller();

		marshaller.setAliases(aliases);

		marshaller.afterPropertiesSet();

		return marshaller;
	}

	@Bean
	@StepScope
	public JdbcPagingItemReader<Target> targetItemReader(DataSource dataSource,
			@Value("#{stepExecutionContext['minValue']}") long minId,
			@Value("#{stepExecutionContext['maxValue']}") long maxId) throws Exception {
		Map<String, Object> parameterValues = new HashMap<>();

		parameterValues.put("minId", minId);
		parameterValues.put("maxId", maxId);

		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("ID", Order.ASCENDING);

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();

		queryProvider.setSelectClause("ID, IP, PORT, CONNECTED, BANNER");
		queryProvider.setFromClause("FROM TARGET");
		queryProvider.setWhereClause("ID >= :minId AND ID <= :maxId AND CONNECTED IS NULL");
		queryProvider.setSortKeys(sortKeys);

		JdbcPagingItemReader<Target> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setQueryProvider(queryProvider);
		reader.setPageSize(10);
		reader.setParameterValues(parameterValues);
		reader.setRowMapper(new TargetRowMapper());

		reader.afterPropertiesSet();

		return reader;
	}

	@Bean
	public TargetScanItemProcessor targetProcessor() {
		return new TargetScanItemProcessor();
	}

	@Bean
	@StepScope
	public JdbcBatchItemWriter<Target> targetWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Target> writer = new JdbcBatchItemWriter<>();

		writer.setAssertUpdates(true);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		writer.setSql("UPDATE TARGET SET CONNECTED = :connected, BANNER = :banner WHERE ID = :id");
		writer.setDataSource(dataSource);

		writer.afterPropertiesSet();

		return writer;
	}

	@Bean
	public Step scanPorts() throws Exception {
		return stepBuilderFactory.get("scanPorts")
				.<Target, Target>chunk(10)
				.reader(targetItemReader(null, 1l, 1l))
				.processor(targetProcessor())
				.writer(targetWriter(null))
				.faultTolerant()
				.skipLimit(20)
				.skip(Exception.class)
				.build();
	}

	@Bean
	public Step loadPorts() {
		return stepBuilderFactory.get("loadPorts")
				.tasklet(loadPortsTasklet(null, null))
				.build();
	}

	@Bean
	public Step masterScanPorts() throws Exception {
		TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();

		partitionHandler.setTaskExecutor(new SimpleAsyncTaskExecutor());
		partitionHandler.setStep(scanPorts());

		partitionHandler.afterPropertiesSet();

		return stepBuilderFactory.get("masterScanPorts")
				.partitioner("scanPorts", partitioner(null))
				.partitionHandler(partitionHandler)
				.gridSize(32)
				.build();
	}

	@Bean
	public Step generateResults() throws Exception {
		return stepBuilderFactory.get("generateResults")
				.<Target, Target>chunk(100)
				.reader(exportItemReader(null))
				.writer(xmlOutputWriter(null, null))
				.build();
	}

	@Bean
	public Job portScannerJob() throws Exception {
		return jobBuilderFactory.get("portScannerJob")
				.incrementer(new RunIdIncrementer())
				.start(loadPorts())
				.next(masterScanPorts())
				.next(generateResults())
				.build();
	}
}
