package net.lecousin.ant.connector.database.r2dbc;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration(exclude = R2dbcAutoConfiguration.class)
public class R2dbcConnectorConfiguration {

}
