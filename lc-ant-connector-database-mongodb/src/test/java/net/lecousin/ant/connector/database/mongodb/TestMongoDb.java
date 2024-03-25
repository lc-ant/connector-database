package net.lecousin.ant.connector.database.mongodb;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.flapdoodle.embed.mongo.spring.autoconfigure.EmbeddedMongoAutoConfiguration;
import lombok.Setter;
import net.lecousin.ant.connector.database.tests.AbstractDatabaseConnectorTest;

@Import({EmbeddedMongoAutoConfiguration.class})
public class TestMongoDb extends AbstractDatabaseConnectorTest implements ApplicationContextAware {
	
	@Setter
	private ApplicationContext applicationContext;
	
	@Autowired @Lazy private MongoProperties properties;
	
	@Override
	protected String getImplementationName() {
		return "mongodb";
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected Map<String, Object> getProperties() {
		Map<String, Object> map = new ObjectMapper().convertValue(properties, Map.class);
		map.put("database", "test");
		return map;
	}
}
