package net.lecousin.ant.connector.database.postgresql.mappers;

import java.lang.reflect.Type;

import com.fasterxml.jackson.core.type.TypeReference;

import io.r2dbc.postgresql.codec.Json;
import net.lecousin.ant.core.mapping.GenericMapper;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.reflection.ResolvedType;
import net.lecousin.ant.core.utils.OptionalNullable;

public class PostgresqlJsonMapper implements GenericMapper {

	@Override
	public boolean canMap(ResolvedType from, ResolvedType to) {
		if (from instanceof ResolvedType.SingleClass c && Json.class.isAssignableFrom(c.getSingleClass())) {
			return true;
		}
		return false;
	}
	
	@Override
	public OptionalNullable<Object> map(ResolvedType sourceType, Object sourceValue, ResolvedType targetType) {
		if (!(sourceValue instanceof Json)) return OptionalNullable.empty();
		Json json = (Json) sourceValue;
		try {
			return OptionalNullable.of(
				Mappers.OBJECT_MAPPER.readValue(json.asArray(), new TypeReference<Object>() {
					@Override
					public Type getType() {
						return targetType.toJavaType();
					}
				})
			);
		} catch (Exception e) {
			return OptionalNullable.empty();
		}
	}
	
}
