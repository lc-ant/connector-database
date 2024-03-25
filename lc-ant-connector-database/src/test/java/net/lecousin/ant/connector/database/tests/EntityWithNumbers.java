package net.lecousin.ant.connector.database.tests;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.core.expression.impl.NumberFieldReference;

@Entity(domain = "test", name = "numbers")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityWithNumbers {
	
	public static final NumberFieldReference<Short> FIELD_S = new NumberFieldReference<>("s");
	public static final NumberFieldReference<Integer> FIELD_I = new NumberFieldReference<>("i");

	@Id @GeneratedValue
	private String id;
	
	private byte b;
	private Byte bo;
	private short s;
	private Short so;
	private int i;
	private Integer io;
	private long l;
	private Long lo;
	private float f;
	private Float fo;
	private double d;
	private Double dob;
	
}
