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
import io.spring.batch.domain.Vote;
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
public class VoteConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public FileSystem fileSystem;

	@Bean
	public JdbcPagingItemReader<Vote> reader() throws Exception {
		JdbcPagingItemReader<Vote> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setFetchSize(1000);
		reader.setQueryProvider(provider());
		reader.setRowMapper(rowMapper());

		reader.afterPropertiesSet();

		return reader;
	}

	@Bean
	public HdfsTextItemWriter<Vote> voteWriter() throws Exception {
		HdfsTextItemWriter<Vote> writer = new HdfsTextItemWriter<>(fileSystem);

		writer.setLineAggregator(new JsonLineAggregator<>());
		writer.setBasePath("/wnb/");
		writer.setBaseFilename("vote");
		writer.setFileSuffix("json");

		return writer;
	}

	public PagingQueryProvider provider() {
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("id", Order.ASCENDING);

		MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();

		provider.setSelectClause("select id, version, post_id, vote_type, creation_date");
		provider.setFromClause("votes");
		provider.setSortKeys(sortKeys);

		return provider;
	}

	public RowMapper<Vote> rowMapper() {
		return new RowMapper<Vote>() {
			@Override
			public Vote mapRow(ResultSet resultSet, int i) throws SQLException {
				Vote vote = new Vote();

				vote.setId(resultSet.getLong("id"));
				vote.setVersion(resultSet.getLong("version"));
				vote.setPostId(resultSet.getLong("post_id"));
				vote.setVoteType(resultSet.getInt("vote_type"));
				vote.setCreationDate(resultSet.getDate("creation_date"));

				return vote;
			}
		};
	}

	@Bean
	public Step voteImport() throws Exception {
		return stepBuilderFactory.get("voteImport")
				.<Vote, Vote>chunk(1000)
				.reader(reader())
				.writer(voteWriter())
				.build();
	}
}
