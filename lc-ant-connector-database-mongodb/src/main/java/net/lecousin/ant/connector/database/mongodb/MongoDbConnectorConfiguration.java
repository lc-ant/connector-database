package net.lecousin.ant.connector.database.mongodb;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoReactiveDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoReactiveRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration(exclude = {
	MongoAutoConfiguration.class,
	MongoReactiveAutoConfiguration.class,
	MongoDataAutoConfiguration.class,
	MongoReactiveDataAutoConfiguration.class,
	MongoRepositoriesAutoConfiguration.class,
	MongoReactiveRepositoriesAutoConfiguration.class,
})
@EnableConfigurationProperties(MongoProperties.class)
public class MongoDbConnectorConfiguration {

}
