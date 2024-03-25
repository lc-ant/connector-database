package net.lecousin.ant.connector.database.tests;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;

@Entity(domain = "test", name = "temporals")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityWithTemporals {

	@Id @GeneratedValue
	private String id;
	
	private Instant instant;
	private LocalDate localDate;
	private LocalTime localTime;
	private LocalDateTime localDateTime;
	
}
