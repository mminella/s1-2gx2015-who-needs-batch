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
package io.spring.batch.workflow.components;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.util.Assert;

/**
 * @author Michael Minella
 */
public class IngestionDecider implements JobExecutionDecider {

	private FileSystem fileSystem;

	private static final long ONE_DAY = 1000 * 60 * 60 * 24;
//	private static final long ONE_DAY = 1000 * 60; // T

	private static final long SIX_DAYS = 1000 * 60 * 60 * 24 * 6;

	public IngestionDecider(FileSystem fileSystem) {
		Assert.notNull(fileSystem);

		this.fileSystem = fileSystem;
	}

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
		String inputDir = jobExecution.getJobParameters().getString("inputDir");

		boolean inputDirExists = false;
		long dateDifference = -1;
		long fileCount = -1;

		try {
			Path path = new Path(inputDir);
			inputDirExists = fileSystem.exists(path);
			FileStatus status = fileSystem.getFileStatus(path);
			Date lastModifiedDate =  new Date(status.getModificationTime());
			Date today = new Date();
			dateDifference = today.getTime() - lastModifiedDate.getTime();
			fileCount = fileSystem.getContentSummary(path).getFileCount();

			jobExecution.getExecutionContext().put("numberOfFiles", fileCount);
			jobExecution.getExecutionContext().put("directoryAge", dateDifference / ONE_DAY);
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		if(!inputDirExists || (dateDifference < ONE_DAY && fileCount < 24)) {
			return new FlowExecutionStatus("END");
		}
		else if(fileCount > 23 || dateDifference > SIX_DAYS) {
			return new FlowExecutionStatus("INGEST");
		}
		else {
			return new FlowExecutionStatus("EMAIL");
		}
	}
}
