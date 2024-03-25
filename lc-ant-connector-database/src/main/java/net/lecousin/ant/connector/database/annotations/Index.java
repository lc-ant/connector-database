package net.lecousin.ant.connector.database.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.lecousin.ant.connector.database.model.IndexType;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
@Repeatable(Index.List.class)
public @interface Index {

	String name() default "";
	
	String[] fields();
	
	IndexType type();
	
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface List {
		
		Index[] value();
		
	}
	
}
