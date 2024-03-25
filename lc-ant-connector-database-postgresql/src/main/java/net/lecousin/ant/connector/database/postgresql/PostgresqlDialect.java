package net.lecousin.ant.connector.database.postgresql;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Comparison;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.RenderContext;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.r2dbc.postgresql.codec.Json;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.model.EntityIndex;
import net.lecousin.ant.connector.database.postgresql.sql.TextSearchCondition;
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
	
	@Override
	protected String createTextIndex(Table table, String name, List<String> fields) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE INDEX ").append(name).append(" ON ").append(table.getName().toSql(processing)).append(" USING GIN (to_tsvector('simple', ");
		for (int i = 0; i < fields.size(); ++i) {
			if (i > 0) sql.append("|| ' ' || ");
			sql.append("coalesce(").append(SqlIdentifier.quoted(fields.get(i)).toSql(processing)).append(", '') ");
		}
		sql.append("))");
		return sql.toString();
	}
	
	@Override
	public Condition buildTextSearchCondition(RenderContext renderContext, EntityMeta meta, String tableName, EntityIndex textIndex, String textToSearch, SqlQuery<?> query) {
		Table table = Table.create(tableName);
		List<Column> columns = textIndex.getFields().stream().map(name -> Column.create(name, table)).toList();
		return Conditions.just(new TextSearchCondition(renderContext, columns, textToSearch).toString());
	}
	
	@Override
	public Condition conditionRegexpMatch(Expression toMatch, Expression regexp) {
		return Comparison.create(toMatch, "~", regexp);
	}
}
