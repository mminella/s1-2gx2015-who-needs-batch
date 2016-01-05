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

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

/**
 * @author Gunnar Hillert
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Jdbc2HdfsSample {

		public static void main(String[] args) throws Exception {

			final boolean writeToLocalFs;

			if(args.length > 0 && args[0].equalsIgnoreCase("localfs")) {
				writeToLocalFs = true;
			}
			else {
				writeToLocalFs = false;
			}

			// set up the execution environment
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			final DataSet<Tuple14<Long, Long, Integer, Long, String, Integer, Integer,
				String, Long, String, Integer, Integer, Integer, Long>> dataSetPosts =
					env.createInput(getInputFormat("select ID, VERSION, POST_TYPE, ACCEPTED_ANSWER_ID,"
							+ "CREATION_DATE, SCORE, VIEW_COUNT, BODY, OWNER_USER_ID, TITLE,"
							+ "ANSWER_COUNT, COMMENT_COUNT, FAVORITE_COUNT, PARENT_ID from POST"),
					new TupleTypeInfo(
							Tuple14.class,
							BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
							BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
							BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
							BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

			dataSetPosts.output(getOutFormat("posts.csv", writeToLocalFs));


			final DataSet<Tuple7<Long, Long, Long, String, String, Long, Integer>> dataSetComments =
					env.createInput(getInputFormat("select ID, VERSION, POST_ID, VALUE, CREATION_DATE, USER_ID, SCORE from COMMENTS"),
					new TupleTypeInfo(
							Tuple7.class, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
							BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

			dataSetComments.output(getOutFormat("comments.csv", writeToLocalFs));

			final DataSet<Tuple2<Long, Long>> dataSetPostTag = env.createInput(getInputFormat("select POST_ID, TAG_ID from POST_TAG"),
					new TupleTypeInfo(
							Tuple2.class, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

			dataSetPostTag.output(getOutFormat("post-tag.csv", writeToLocalFs));

			final DataSet<Tuple3<Long, Long, String>> dataSetTag = env.createInput(getInputFormat("select ID, VERSION, TAG from TAG"),
					new TupleTypeInfo(
							Tuple3.class, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

			dataSetTag.output(getOutFormat("tag.csv", writeToLocalFs));

			final DataSet<Tuple11<Long, Long, Integer, String, String, String, String, String, Integer, Integer, Integer>> dataSetUsers = env.createInput(getInputFormat("select ID, VERSION, REPUTATION, CREATION_DATE, DISPLAY_NAME, LAST_ACCESS_DATE, LOCATION, ABOUT, VIEWS, UP_VOTES, DOWN_VOTES from USERS"),
					new TupleTypeInfo(
							Tuple11.class,
							BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
							BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
							BasicTypeInfo.INT_TYPE_INFO));

			dataSetUsers.output(getOutFormat("users.csv", writeToLocalFs));

			final DataSet<Tuple5<Long, Long, Long, Integer, String>> dataSetVotes = env.createInput(getInputFormat("select ID, VERSION, POST_ID, VOTE_TYPE, CREATION_DATE from VOTES"),
					new TupleTypeInfo(
							Tuple5.class, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

			dataSetVotes.output(getOutFormat("votes.csv", writeToLocalFs));

			//DataSet Transformation
			final DataSet top5Tags = dataSetPostTag.groupBy(1)
				.reduceGroup(new GroupReduceFunction<Tuple2<Long,Long>, Tuple2<Long,Long>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long,Long>> out) throws Exception {
						for (Tuple2<Long, Long> tuple : values) {
							out.collect(new Tuple2<Long, Long>(tuple.f1, 1L));
						}
					}

				}).groupBy(0).sum(1).sortPartition(1, Order.DESCENDING)
					.first(5).join(dataSetTag)
					.where(0).equalTo(0)
					.projectFirst(1).projectSecond(2);

			top5Tags.output(getOutFormat("top5Tags.csv", writeToLocalFs));
			JobExecutionResult executionResult = env.execute();

			System.out.println("Finished in " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms.");
		}

		private static JDBCInputFormat getInputFormat(String query) {
			return JDBCInputFormat.buildJDBCInputFormat()
			.setDBUrl("jdbc:mysql://127.0.0.1:3306/bicycles")
			.setDrivername("com.mysql.jdbc.Driver")
			.setUsername("root")
			.setPassword("p@ssw0rd")
			.setQuery(query).finish();
		}

		private static CsvOutputFormat getOutFormat(String filename, boolean writeToLocalFs) {

			final Path outputPath;

			if (writeToLocalFs) {
				outputPath = new Path("file:///tmp/flink/" + filename);
			}
			else {
				outputPath = new Path("hdfs://localhost:8020/flink/" + filename);
			}
			final CsvOutputFormat outputFormat = new CsvOutputFormat(outputPath, "\n", "|");

			outputFormat.setAllowNullValues(true);
			outputFormat.setWriteMode(WriteMode.OVERWRITE);
			return outputFormat;
		}
}
