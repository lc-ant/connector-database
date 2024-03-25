package net.lecousin.ant.connector.database.tests;

import java.util.Optional;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;

@Entity(domain = "test", name = "optional")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityWithOptional {

	@Id @GeneratedValue
	private String id;

	private Optional<String> str;
	
}
