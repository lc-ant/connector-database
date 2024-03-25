package net.lecousin.ant.connector.database.tests;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import net.lecousin.ant.connector.database.DatabaseConnectorConfiguration;

@Configuration
@EnableAutoConfiguration
@Import(DatabaseConnectorConfiguration.class)
public class DatabaseConnectorTestConfig {

}
