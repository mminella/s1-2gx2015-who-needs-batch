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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import io.spring.batch.components.HdfsTextItemWriter;
import io.spring.batch.domain.Comment;
import org.apache.hadoop.fs.FileSystem;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

/**
 * @author Michael Minella
 */
@Configuration
public class CommentConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public FileSystem fileSystem;

	@Bean
	public JdbcPagingItemReader<Comment> commentReader() throws Exception {
		JdbcPagingItemReader<Comment> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setFetchSize(1000);
		reader.setQueryProvider(provider());
		reader.setRowMapper(rowMapper());

		reader.afterPropertiesSet();

		return reader;
	}

	@Bean
	public HdfsTextItemWriter<Comment> commentWriter() throws Exception {
		HdfsTextItemWriter<Comment> writer = new HdfsTextItemWriter<>(fileSystem);

		writer.setLineAggregator(new DelimitedLineAggregator<>());
		writer.setBasePath("/wnb/");
		writer.setBaseFilename("comment");
		writer.setFileSuffix("csv");

		return writer;
	}

	public PagingQueryProvider provider() {
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("id", Order.ASCENDING);

		MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();

		provider.setSelectClause("select id, version, post_id, value, creation_date, user_id, score");
		provider.setFromClause("comments");
		provider.setSortKeys(sortKeys);

		return provider;
	}

	public RowMapper<Comment> rowMapper() {
		return new RowMapper<Comment>() {
			@Override
			public Comment mapRow(ResultSet resultSet, int i) throws SQLException {
				Comment comment = new Comment();

				comment.setId(resultSet.getLong("id"));
				comment.setVersion(resultSet.getLong("version"));
				comment.setPostId(resultSet.getLong("post_id"));
				comment.setValue(resultSet.getString("value"));
				comment.setCreationDate(resultSet.getDate("creation_date"));
				comment.setUserId(resultSet.getLong("user_id"));
				comment.setScore(resultSet.getInt("score"));

				return comment;
			}
		};
	}

	@Bean
	public Step commentImport() throws Exception {
		return stepBuilderFactory.get("commentImport")
				.<Comment, Comment>chunk(1000)
				.reader(commentReader())
				.writer(commentWriter())
				.build();
	}

}
