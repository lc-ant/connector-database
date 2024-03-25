package net.lecousin.ant.connector.database.mongodb;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.springboot.connector.ConnectorFactory;
import net.lecousin.ant.core.springboot.utils.SpringEnvironmentUtils;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class MongoDbConnectorFactory implements ConnectorFactory<MongoDbConnector, MongoProperties>, ApplicationContextAware {

	@Autowired(required = false)
	@Lazy
	private MongoProperties properties;
	
	@Setter
	private ApplicationContext applicationContext;
	
	@Override
	public String getType() {
		return "database";
	}
	
	@Override
	public String getImplementation() {
		return "mongodb";
	}
	
	@Override
	public Class<MongoDbConnector> getConnectorClass() {
		return MongoDbConnector.class;
	}
	
	@Override
	public Class<MongoProperties> getPropertiesClass() {
		return MongoProperties.class;
	}
	
	@Override
	public Mono<MongoDbConnector> create(MongoProperties mongoProperties) {
		return Mono.fromCallable(() -> {
			MongoProperties props = Optional.ofNullable(mongoProperties)
				.or(() -> {
					Map<String, Serializable> map = SpringEnvironmentUtils.getPropertyAsMap(applicationContext.getEnvironment(), "lc-ant.connector.database.mongodb");
					if (map == null) return Optional.empty();
					return Optional.of(Mappers.OBJECT_MAPPER.convertValue(map, MongoProperties.class));
				})
				.orElse(this.properties);
			log.info("Connect MongoDB Database connector with properties: {}", Mappers.OBJECT_MAPPER.convertValue(props, Map.class));
			MongoClientSettings.Builder builder = MongoClientSettings.builder();
			if (props.getUsername() != null && props.getPassword() != null) {
				builder.credential(MongoCredential.createCredential(props.getUsername(), props.getAuthenticationDatabase(), props.getPassword()));
			}
			if (props.getHost() != null && props.getPort() != null) {
				builder.applyToClusterSettings(b -> b.hosts(List.of(new ServerAddress(props.getHost(), props.getPort()))));
			}
			
			MongoClient client = MongoClients.create(builder.build());
			ReactiveMongoTemplate template = new ReactiveMongoTemplate(client, props.getDatabase());
			return new MongoDbConnector(client, template);
		});
	}
	
}
