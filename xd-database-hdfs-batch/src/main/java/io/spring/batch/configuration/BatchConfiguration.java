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
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.fs.FileSystemFactoryBean;


/**
 * @author Michael Minella
 * @author Gunnar Hillert
 */
@Configuration
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("postImport")
	private Step postStep;

	@Autowired
	@Qualifier("commentImport")
	private Step commentStep;

	@Autowired
	@Qualifier("userImport")
	private Step userStep;

	@Autowired
	@Qualifier("voteImport")
	private Step voteStep;

	@Autowired
	org.apache.hadoop.conf.Configuration configuration;

	@Bean
	FileSystemFactoryBean hadoopFs() {
		FileSystemFactoryBean fb = new FileSystemFactoryBean();
		fb.setConfiguration(this.configuration);
		return fb;
	}

	@Bean
	public Job job() {
		return jobBuilderFactory.get("job")
				.start(postStep)
				.next(commentStep)
				.next(userStep)
				.next(voteStep)
				.build();
	}

}
