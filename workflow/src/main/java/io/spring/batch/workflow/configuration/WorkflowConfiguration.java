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

import java.util.Map;

import io.spring.batch.workflow.components.IngestionDecider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

/**
 * General path
 *
 * java --> decision --> end
 *                   \-> ingest    --> archive-data --> end
 *                   \-> sendEmail  \-> ingest-fail  \-> ingest-fail
 *
 *
 *  Spring Batch flow
 *
 *  Decision --> ingest    -> archive-data
 *           \-> sendEmail
 *
 * @author Michael Minella
 */
@Configuration
public class WorkflowConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Value("${job.email.to}")
	private String toAddress;

	@Autowired
	public FileSystem fileSystem;

	@Bean
	public JobExecutionDecider decider() {
		return new IngestionDecider(fileSystem);
	}

	@Bean
	@StepScope
	public Tasklet emailTasklet(JavaMailSender mailSender) {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				Map<String, Object> jobExecutionContext = chunkContext.getStepContext().getJobExecutionContext();

				String inputDir = (String) chunkContext.getStepContext().getJobParameters().get("inputDir");

				SimpleMailMessage mail = new SimpleMailMessage();

				mail.setTo(toAddress);

				mail.setSubject(String.format("Directory %s contains %d files", inputDir, (long) jobExecutionContext.get("numberOfFiles")));
				mail.setText(String.format("Directory %s is %d minutes old and contains only %d files instead of 24.",
						inputDir,
						(long) jobExecutionContext.get("directoryAge"),
						(long) jobExecutionContext.get("numberOfFiles")));

				mailSender.send(mail);

				return RepeatStatus.FINISHED;
			}
		};
	}

	@Bean
	public Step emailStep(@Qualifier("emailTasklet") Tasklet tasklet) {
		return stepBuilderFactory.get("emailStep")
				.tasklet(tasklet)
				.build();
	}

	@Bean
	@StepScope
	public Tasklet ingestTasklet(FileSystem fileSystem) {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				String inputDir = (String) chunkContext.getStepContext().getJobParameters().get("inputDir");
				String ingestedDir = "ingested";

				if(inputDir.endsWith("/")) {
					inputDir = inputDir.substring(0, inputDir.length() - 1);
				}

				String outputDir = inputDir + "ingested";

				System.out.println(">> INPUT DIR = " + inputDir + " OUTPUT DIR = " + outputDir);

				FileUtil.copyMerge(fileSystem, new Path(inputDir), fileSystem, new Path(outputDir + "/ingested.out"), false, fileSystem.getConf(), "");
				return RepeatStatus.FINISHED;
			}
		};
	}

	@Bean
	public Step ingestStep(@Qualifier("ingestTasklet") Tasklet tasklet) {
		return stepBuilderFactory.get("ingestStep")
				.tasklet(tasklet)
				.build();
	}

	@Bean
	@StepScope
	public Tasklet archiveTasklet(FileSystem fileSystem,
			@Value("#jobParameters['inputDir']") String inputDir,
			@Value("#jobParameters['archiveDir'") String archiveDir) {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				FileUtil.copy(fileSystem, new Path(inputDir), fileSystem, new Path("/probe/archive/" + archiveDir), true, fileSystem.getConf());
				return RepeatStatus.FINISHED;
			}
		};
	}

	@Bean
	public Step archiveStep(@Qualifier("archiveTasklet") Tasklet tasklet) {
		return stepBuilderFactory.get("ingestStep")
				.tasklet(tasklet)
				.build();
	}

	@Bean
	public Job ingestJob(@Qualifier("emailStep") Step emailStep,
			@Qualifier("ingestStep") Step ingestStep) {
		FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("mainFlow");

		Flow flow = flowBuilder.start(decider())
				.on("END").end()
				.from(decider()).on("EMAIL").to(emailStep)
				.from(decider()).on("INGEST").to(ingestStep)
				.from(emailStep)
					.on("FAIL").fail()
					.on("*").end()
				.from(ingestStep)
					.on("FAIL").fail()
					.on("*").end()
				.build();

		return jobBuilderFactory.get("ingestJob")
				.start(flow)
				.end()
				.build();
	}
}
