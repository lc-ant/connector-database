package net.lecousin.ant.connector.database.postgresql;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.lecousin.ant.connector.database.tests.AbstractDatabaseConnectorTest;

public class TestPostgresqlService extends AbstractDatabaseConnectorTest {

	@Autowired private PostgreSQLProperties properties;
	
	@Override
	protected String getImplementationName() {
		return "postgresql";
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected Map<String, Object> getProperties() {
		return new ObjectMapper().convertValue(properties, Map.class);
	}
	
}
