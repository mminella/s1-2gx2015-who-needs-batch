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
package io.spring.batch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * @author Michael Minella
 */
public class Main {

	public static void main(String[] args) {
		// URL of the Spark cluster
		String sparkUrl = "local[2]";

		SparkConf conf = new SparkConf();

		conf.setMaster(sparkUrl);
		conf.setAppName("Twitter");
		conf.validateSettings();

		JavaStreamingContext ssc = new JavaStreamingContext(sparkUrl, "Twitter", new Duration(1000));

		Configuration configuration = new ConfigurationBuilder()
				.setOAuthConsumerKey(System.getProperty("twitter4j.oauth.consumerKey"))
				.setOAuthConsumerSecret(System.getProperty("twitter4j.oauth.consumerSecret"))
				.setOAuthAccessToken(System.getProperty("twitter4j.oauth.accessKey"))
				.setOAuthAccessTokenSecret(System.getProperty("twitter4j.oauth.accessSecret"))
				.setDebugEnabled(true)
				.build();

		JavaDStream<Status> tweets = TwitterUtils.createStream(ssc, new OAuthAuthorization(configuration));

		tweets.map(
				new Function<Status, String>() {
					public String call(Status status) {
						return status.getText();
					}
				}
		).dstream().saveAsTextFiles("hdfs://localhost:8020/spark/twitter/", "txt");

		ssc.start();

		// Just run stream for 20 seconds
		ssc.awaitTermination(20000);
	}
}
