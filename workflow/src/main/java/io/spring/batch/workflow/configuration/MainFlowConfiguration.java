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
package io.spring.batch.workflow.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.job.JobParametersExtractor;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * @author Michael Minella
 */
@Configuration
public class MainFlowConfiguration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	@Qualifier("ingestJob")
	public Job ingestJob;

	@Bean
	public Partitioner partitioner(FileSystem fileSystem) {
		return new Partitioner() {
			@Override
			public Map<String, ExecutionContext> partition(int gridSize) {
				Map<String, ExecutionContext> contexts = new HashMap<>();

				try {
					FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/probes"), new PathFilter() {
						@Override
						public boolean accept(Path path) {
							try {
								return fileSystem.isDirectory(path);
							}
							catch (IOException e) {
								return false;
							}
						}
					});

					int count = 0;
					for (FileStatus fileStatus : fileStatuses) {
						ExecutionContext executionContext = new ExecutionContext();

						executionContext.put("curInputDir", fileStatus.getPath().toString());

						contexts.put("dir" + count, executionContext);

						count++;
					}

				}
				catch (IOException e) {
					e.printStackTrace();
				}

				return contexts;
			}
		};
	}

	@Bean
	public Step partitionedStep() throws Exception {
		TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();

		partitionHandler.setStep(ingestFlow());
		partitionHandler.setTaskExecutor(new SimpleAsyncTaskExecutor());

		partitionHandler.afterPropertiesSet();

		return stepBuilderFactory.get("mainStep")
				.partitioner("ingestFlow", partitioner(null))
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean
	public Step ingestFlow() {
		return stepBuilderFactory.get("ingestFlow")
				.job(ingestJob)
				.parametersExtractor(new JobParametersExtractor() {
					@Override
					public JobParameters getJobParameters(Job job, StepExecution stepExecution) {
						JobParametersBuilder jobParametersBuilder = new JobParametersBuilder(stepExecution.getJobParameters());

						jobParametersBuilder.addString("inputDir", stepExecution.getExecutionContext().getString("curInputDir"));

						return jobParametersBuilder.toJobParameters();
					}
				}).build();
	}

	@Bean
	public Job mainJob() throws Exception {
		return jobBuilderFactory.get("mainJob")
				.incrementer(new RunIdIncrementer())
				.start(partitionedStep())
				.build();
	}
}
