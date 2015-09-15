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
