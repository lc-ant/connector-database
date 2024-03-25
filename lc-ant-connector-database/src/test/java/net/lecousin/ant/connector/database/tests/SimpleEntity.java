package net.lecousin.ant.connector.database.tests;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.core.expression.impl.NumberFieldReference;

@Entity(domain = "test", name = "simple")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SimpleEntity {
	
	public static final NumberFieldReference<Integer> FIELD_INTEGER = new NumberFieldReference<>("integer");

	@Id @GeneratedValue
	private String id;
	
	private int integer;
	
}
