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

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * @author Michael Minella
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job job(@Qualifier("postImport") Step postStep,
			@Qualifier("commentImport") Step commentStep,
			@Qualifier("userImport") Step userStep,
			@Qualifier("voteImport") Step voteStep) {

		FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("split");

		Flow splitFlow = flowBuilder.split(new SimpleAsyncTaskExecutor())
				.add(new FlowBuilder<Flow>("postFlow").start(postStep).build(),
						new FlowBuilder<Flow>("commentFlow").start(commentStep).build(),
						new FlowBuilder<Flow>("userFlow").start(userStep).build(),
						new FlowBuilder<Flow>("voteFlow").start(voteStep).build())
				.build();

		return jobBuilderFactory.get("importJob")
				.incrementer(new RunIdIncrementer())
				.start(splitFlow)
				.end()
				.build();
	}
}
