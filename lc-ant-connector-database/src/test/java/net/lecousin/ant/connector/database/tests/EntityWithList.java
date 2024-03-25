package net.lecousin.ant.connector.database.tests;

import java.util.List;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.core.expression.impl.CollectionFieldReference;
import net.lecousin.ant.core.expression.impl.NumberFieldReference;

@Entity(domain = "test", name = "list")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityWithList {
	
	public static final CollectionFieldReference<String, ?> FIELD_VALUES = new CollectionFieldReference<>("values");
	public static final NumberFieldReference<Integer> FIELD_I = new NumberFieldReference<>("i");

	@Id @GeneratedValue
	private String id;
	
	private List<String> values;
	private int i;
	
}
