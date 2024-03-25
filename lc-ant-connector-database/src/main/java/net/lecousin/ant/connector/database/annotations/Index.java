package net.lecousin.ant.connector.database.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
@Repeatable(Index.List.class)
public @interface Index {

	String name() default "";
	
	String[] fields();
	
	boolean unique() default false;
	
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface List {
		
		Index[] value();
		
	}
	
}
