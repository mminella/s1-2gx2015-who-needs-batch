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
import io.spring.batch.domain.JsonLineAggregator;
import io.spring.batch.domain.Post;
import org.apache.hadoop.fs.FileSystem;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

/**
 * @author Michael Minella
 */
@Configuration
public class PostConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public FileSystem fileSystem;

	@Bean
	public JdbcPagingItemReader<Post> postReader() throws Exception {
		JdbcPagingItemReader<Post> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setFetchSize(1000);
		reader.setQueryProvider(provider());
		reader.setRowMapper(rowMapper());

		reader.afterPropertiesSet();

		return reader;
	}

	@Bean
	public HdfsTextItemWriter<Post> postWriter() throws Exception {
		HdfsTextItemWriter<Post> writer = new HdfsTextItemWriter<>(fileSystem);

		writer.setLineAggregator(new JsonLineAggregator<>());
		writer.setBasePath("/wnb/");
		writer.setBaseFilename("post");
		writer.setFileSuffix("json");

		return writer;
	}

	public PagingQueryProvider provider() {
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("id", Order.ASCENDING);

		MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();

		provider.setSelectClause("select id, version, post_type, accepted_answer_id, creation_date, score, view_count, body, owner_user_id, title, answer_count, comment_count, favorite_count, parent_id");
		provider.setFromClause("post");
		provider.setSortKeys(sortKeys);

		return provider;
	}

	public RowMapper<Post> rowMapper() {
		return new RowMapper<Post>() {
			@Override
			public Post mapRow(ResultSet resultSet, int i) throws SQLException {
				Post post = new Post();

				post.setId(resultSet.getLong("id"));
				post.setVersion(resultSet.getLong("version"));
				post.setPostTypeId(resultSet.getInt("post_type"));
				post.setAcceptedAnswerId(resultSet.getLong("accepted_answer_id"));
				post.setCreationDate(resultSet.getDate("creation_date"));
				post.setScore(resultSet.getInt("score"));
				post.setViewCount(resultSet.getInt("view_count"));
				post.setBody(resultSet.getString("body"));
				post.setOwnerUserId(resultSet.getLong("owner_user_id"));
				post.setTitle(resultSet.getString("title"));
				post.setAnswerCount(resultSet.getInt("answer_count"));
				post.setCommentCount(resultSet.getInt("comment_count"));
				post.setFavoriteCount(resultSet.getInt("favorite_count"));
				post.setParentId(resultSet.getLong("parent_id"));

				return post;
			}
		};
	}

	@Bean
	public Step postImport() throws Exception {
		return stepBuilderFactory.get("postImport")
				.<Post, Post>chunk(1000)
				.reader(postReader())
				.writer(postWriter())
				.build();
	}
}
