package net.lecousin.ant.connector.database.postgresql;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import net.lecousin.ant.connector.database.r2dbc.R2dbcConnectorConfiguration;

@Configuration
@Import(R2dbcConnectorConfiguration.class)
public class PostgresSQLConnectorConfiguration {

}
