package net.lecousin.ant.connector.database.postgresql;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;

import io.r2dbc.spi.ConnectionFactories;
import lombok.Setter;
import net.lecousin.ant.core.springboot.connector.ConnectorFactory;
import reactor.core.publisher.Mono;

@Service
public class PostgreSQLConnectorFactory implements ConnectorFactory<PostgreSQLConnector, PostgreSQLProperties>, ApplicationContextAware {

	@Autowired(required = false)
	@Lazy
	private DataSourceProperties properties;
	
	@Setter
	private ApplicationContext applicationContext;
	
	@Override
	public String getType() {
		return "database";
	}
	
	@Override
	public String getImplementation() {
		return "postgresql";
	}
	
	@Override
	public Class<PostgreSQLConnector> getConnectorClass() {
		return PostgreSQLConnector.class;
	}
	
	@Override
	public Class<PostgreSQLProperties> getPropertiesClass() {
		return PostgreSQLProperties.class;
	}
	
	@Override
	public Mono<PostgreSQLConnector> create(PostgreSQLProperties dataSourceProperties) {
		return Mono.fromCallable(() -> {
			String url = Optional.ofNullable(dataSourceProperties).flatMap(p -> Optional.ofNullable(p.getUri()))
				.or(() -> Optional.ofNullable(applicationContext.getEnvironment().getProperty("lc-ant.connector.database.postgresql.url")))
				.orElse(this.properties.getUrl());
			var connectionFactory = ConnectionFactories.get(url);
			var client = DatabaseClient.create(connectionFactory);
			return new PostgreSQLConnector(client);
		});
	}
	
}
