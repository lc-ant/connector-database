package net.lecousin.ant.connector.database.postgresql;

import java.io.IOException;
import java.sql.Connection;
import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

@Configuration
public class PostgresqlTestConfig {

	public static EmbeddedPostgres epg;
	private static Throwable error = null;
	
	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (epg != null)
					try {
						epg.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		});
		EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder().setPGStartupWait(Duration.ofSeconds(30));
		try {
			epg = builder.start();
			Connection conn = epg.getPostgresDatabase().getConnection();
			conn.prepareCall("CREATE DATABASE test").execute();
			conn.close();
		} catch (Throwable e) {
			error = e;
		}
	}
	
	@Bean
	PostgreSQLProperties postgresqlProperties() {
		if (error != null)
			throw new RuntimeException("PostgreSQL server not started", error);
		return new PostgreSQLProperties("r2dbc:pool:postgresql://postgres@localhost:" + epg.getPort());
	}
}
