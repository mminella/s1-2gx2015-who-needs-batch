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
import io.spring.batch.domain.User;
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
public class UserConfiguration {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public FileSystem fileSystem;

	@Autowired
	public DataSource dataSource;

	@Bean
	public JdbcPagingItemReader<User> reader() throws Exception {
		JdbcPagingItemReader<User> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setFetchSize(1000);
		reader.setQueryProvider(provider());
		reader.setRowMapper(rowMapper());

		reader.afterPropertiesSet();

		return reader;
	}

	@Bean
	public HdfsTextItemWriter<User> userWriter() throws Exception {
		HdfsTextItemWriter<User> writer = new HdfsTextItemWriter<>(fileSystem);

		writer.setLineAggregator(new JsonLineAggregator<>());
		writer.setBasePath("/wnb/");
		writer.setBaseFilename("user");
		writer.setFileSuffix("json");

		return writer;
	}

	public PagingQueryProvider provider() {
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("id", Order.ASCENDING);

		MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();

		provider.setSelectClause("select id, version, reputation, creation_date, display_name, last_access_date, location, about, views, up_votes, down_votes");
		provider.setFromClause("users");
		provider.setSortKeys(sortKeys);

		return provider;
	}

	public RowMapper<User> rowMapper() {
		return new RowMapper<User>() {
			@Override
			public User mapRow(ResultSet resultSet, int i) throws SQLException {
				User user = new User();

				user.setId(resultSet.getLong("id"));
				user.setVersion(resultSet.getLong("version"));
				user.setReputation(resultSet.getInt("reputation"));
				user.setCreationDate(resultSet.getDate("creation_date"));
				user.setDisplayName(resultSet.getString("display_name"));
				user.setLastAccessDate(resultSet.getDate("last_access_date"));
				user.setLocation(resultSet.getString("location"));
				user.setAbout(resultSet.getString("about"));
				user.setViews(resultSet.getInt("views"));
				user.setUpVotes(resultSet.getInt("up_votes"));
				user.setDownVotes(resultSet.getInt("down_votes"));

				return user;
			}
		};
	}

	@Bean
	public Step userImport() throws Exception {
		return stepBuilderFactory.get("userImport")
				.<User, User>chunk(1000)
				.reader(reader())
				.writer(userWriter())
				.build();
	}
}
