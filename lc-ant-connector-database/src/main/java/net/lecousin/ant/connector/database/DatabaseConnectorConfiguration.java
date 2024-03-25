package net.lecousin.ant.connector.database;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import net.lecousin.ant.core.springboot.connector.LcAntCoreConnectorConfiguration;

@Configuration
@Import(LcAntCoreConnectorConfiguration.class)
@ComponentScan
public class DatabaseConnectorConfiguration {

}
