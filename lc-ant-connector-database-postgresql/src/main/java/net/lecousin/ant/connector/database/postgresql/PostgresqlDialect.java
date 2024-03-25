package net.lecousin.ant.connector.database.postgresql;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.relational.core.sql.IdentifierProcessing;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.r2dbc.postgresql.codec.Json;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.r2dbc.CriteriaBuilder;
import net.lecousin.ant.connector.database.r2dbc.ExtendedR2dbcDialect;
import net.lecousin.ant.connector.database.r2dbc.SqlQuery;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.reflection.ClassProperty;
import net.lecousin.ant.core.reflection.ResolvedType;

public class PostgresqlDialect extends ExtendedR2dbcDialect {
	
	public PostgresqlDialect(IdentifierProcessing processing) {
		super(processing);
	}
	
	@Override
	protected Optional<ColumnType> getColumnType(ResolvedType type, ClassProperty property) {
		var result = super.getColumnType(type, property);
		if (result.isPresent()) return result;
		var rawOpt = ResolvedType.getRawClass(type);
		if (rawOpt.isPresent()) {
			var raw = rawOpt.get();
			if (Collection.class.isAssignableFrom(raw) || Map.class.isAssignableFrom(raw))
				return Optional.of(ColumnType.builder().type("JSONB").build());
		}
		return Optional.empty();
	}
	
	@Override
	public Object encode(Object value) {
		var c = value.getClass();
		if (Collection.class.isAssignableFrom(c) || Map.class.isAssignableFrom(c))
			try {
				return Json.of(Mappers.OBJECT_MAPPER.writeValueAsBytes(value));
			} catch (JsonProcessingException e) {
				throw new RuntimeException("Cannot encode value to JSON", e);
			}
		return super.encode(value);
	}

	@Override
	public CriteriaBuilder createCriteriaBuilder(EntityMeta meta, String tableName, SqlQuery<?> query) {
		return new PostgreSQLCriteriaBuilder(this, meta, tableName, query);
	}
}
