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
package io.spring.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

/**
 *
 */
public class Jdbc2HdfsSample {

		public static void main(String[] args) throws Exception {

			// set up the execution environment
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Long, Integer, String>> dbData = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
					.setDBUrl("jdbc:mysql://127.0.0.1:3306/batch_demo")
					.setDrivername("com.mysql.jdbc.Driver")
					.setUsername("root")
					.setPassword("root")
					.setQuery("select ID, VIEW_COUNT, TITLE from POST").finish(),
					new TupleTypeInfo(
							Tuple3.class, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

			final CsvOutputFormat<Tuple3<Long, Integer, String>> outputFormat = new CsvOutputFormat<>(new Path("hdfs://localhost:8020/result.csv"), "\n", "|");
			outputFormat.setAllowNullValues(true);
			outputFormat.setWriteMode(WriteMode.OVERWRITE);

			dbData.output(outputFormat);
			env.execute();

		}

}
