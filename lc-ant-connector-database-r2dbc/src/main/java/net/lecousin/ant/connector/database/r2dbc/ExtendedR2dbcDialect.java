package net.lecousin.ant.connector.database.r2dbc;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.RenderContext;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.connector.database.model.EntityIndex;
import net.lecousin.ant.core.reflection.ClassProperty;
import net.lecousin.ant.core.reflection.ReflectionException;
import net.lecousin.ant.core.reflection.ResolvedType;
import net.lecousin.ant.core.validation.annotations.StringConstraint;

public abstract class ExtendedR2dbcDialect {
	
	protected IdentifierProcessing processing;
	
	protected ExtendedR2dbcDialect(IdentifierProcessing processing) {
		this.processing = processing;
	}

	public String createTable(Table table, Collection<ClassProperty> properties) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE IF NOT EXISTS ").append(table.getName().toSql(processing));
		sql.append(" (");
		boolean first = true;
		for (var prop : properties) {
			if (first)
				first = false;
			else
				sql.append(", ");
			addColumnDefinition(prop, sql);
		}
		sql.append(')');
		return sql.toString();
	}
	
	protected void addColumnDefinition(ClassProperty property, StringBuilder sql) {
		var oct = getColumnType(property);
		if (oct.isEmpty()) throw new ReflectionException("Cannot determine column type for property " + property.getName() + " with type " + property.getType());
		var ct = oct.get();
		sql.append(SqlIdentifier.quoted(property.getName()).toSql(processing));
		sql.append(' ');
		sql.append(ct.type);
		if (!ct.isNullable())
			addNotNull(sql);
		if (ct.getAddition() != null)
			sql.append(ct.getAddition());
		if (property.hasAnnotation(Id.class))
			addPrimaryKey(sql);
	}
	
	@Data
	@Builder
	@NoArgsConstructor
	@AllArgsConstructor
	protected static final class ColumnType {
		private String type;
		@Builder.Default
		private boolean nullable = false;
		private String addition;
	}
	
	protected Optional<ColumnType> getColumnType(ClassProperty property) {
		return getColumnType(property.getType(), property);
	}
	
	protected Optional<ColumnType> getColumnType(ResolvedType type, ClassProperty property) {
		var rawOpt = ResolvedType.getRawClass(type);
		if (rawOpt.isPresent()) {
			var raw = rawOpt.get();
			if (byte.class.equals(raw)) return Optional.of(ColumnType.builder().type("SMALLINT").build());
			else if (Byte.class.equals(raw)) return Optional.of(ColumnType.builder().type("SMALLINT").nullable(true).build());
			else if (short.class.equals(raw)) return Optional.of(ColumnType.builder().type("SMALLINT").build());
			else if (Short.class.equals(raw)) return Optional.of(ColumnType.builder().type("SMALLINT").nullable(true).build());
			else if (int.class.equals(raw)) return Optional.of(ColumnType.builder().type("INT").build());
			else if (Integer.class.equals(raw)) return Optional.of(ColumnType.builder().type("INT").nullable(true).build());
			else if (long.class.equals(raw)) return Optional.of(ColumnType.builder().type("BIGINT").build());
			else if (Long.class.equals(raw)) return Optional.of(ColumnType.builder().type("BIGINT").nullable(true).build());
			else if (float.class.equals(raw)) return Optional.of(ColumnType.builder().type("REAL").build());
			else if (Float.class.equals(raw)) return Optional.of(ColumnType.builder().type("REAL").nullable(true).build());
			else if (double.class.equals(raw)) return Optional.of(ColumnType.builder().type("DOUBLE PRECISION").build());
			else if (Double.class.equals(raw)) return Optional.of(ColumnType.builder().type("DOUBLE PRECISION").nullable(true).build());
			else if (String.class.equals(raw)) return getStringColumnType(property);
			else if (Instant.class.equals(raw)) return Optional.of(ColumnType.builder().type("TIMESTAMP(3) WITH TIME ZONE").build());
			else if (Date.class.equals(raw)) return Optional.of(ColumnType.builder().type("TIMESTAMP(3) WITH TIME ZONE").build());
			else if (LocalDateTime.class.equals(raw)) return Optional.of(ColumnType.builder().type("TIMESTAMP(3)").build());
			else if (LocalDate.class.equals(raw)) return Optional.of(ColumnType.builder().type("DATE").build());
			else if (LocalTime.class.equals(raw)) return Optional.of(ColumnType.builder().type("TIME(3)").build());
			else if (Optional.class.equals(raw) && type instanceof ResolvedType.Parameterized p && p.getParameters().length == 1)
				return getColumnType(p.getParameters()[0], property).map(c -> {
					c.setNullable(true);
					return c;
				});
		}
		return Optional.empty();
	}
	
	protected Optional<ColumnType> getStringColumnType(ClassProperty property) {
		if (property.hasAnnotation(GeneratedValue.class))
			return Optional.of(ColumnType.builder().type("UUID").addition(" DEFAULT gen_random_uuid()").build());
		ColumnType c = new ColumnType();
		c.setType("VARCHAR");
		property.getAnnotation(StringConstraint.class).ifPresent(cs -> {
			if (cs.maxLength() > 0)
				c.setType("VARCHAR(" + cs.maxLength() + ")");
		});
		return Optional.of(c);
	}
	
	protected void addNotNull(StringBuilder sql) {
		sql.append(" NOT NULL");
	}
	
	protected void addPrimaryKey(StringBuilder sql) {
		sql.append(" PRIMARY KEY");
	}
	
	public String createIndex(Table table, EntityIndex index) {
		switch (index.getType()) {
		case SIMPLE: return createIndex(table, index.getName(), index.getFields(), false);
		case UNIQUE: return createIndex(table, index.getName(), index.getFields(), true);
		case TEXT: return createTextIndex(table, index.getName(), index.getFields());
		}
		throw new IllegalArgumentException("Unknown index type: " + index.getType());
	}
	
	protected String createIndex(Table table, String name, List<String> fields, boolean unique) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE ");
		if (unique)
			sql.append("UNIQUE ");
		sql.append("INDEX ");
		sql.append(name);
		sql.append(" ON ");
		sql.append(table.getName().toSql(processing));
		sql.append('(');
		for (int i = 0; i < fields.size(); ++i) {
			if (i > 0) sql.append(',');
			sql.append(SqlIdentifier.quoted(fields.get(i)).toSql(processing));
		}
		sql.append(')');
		return sql.toString();
	}
	
	protected String createTextIndex(Table table, String name, List<String> fields) {
		throw new IllegalStateException("Text index not supported");
	}
	
	public Object encode(Object value) {
		return value;
	}
	
	public abstract CriteriaBuilder createCriteriaBuilder(EntityMeta meta, String tableName, SqlQuery<?> query);
	
	public Condition buildTextSearchCondition(RenderContext renderContext, EntityMeta meta, String tableName, EntityIndex textIndex, String textToSearch, SqlQuery<?> query) {
		throw new IllegalStateException("Text index not supported");
	}
	
	public Condition conditionRegexpMatch(Expression toMatch, Expression regexp) {
		Expression function = SimpleFunction.create("REGEXP_LIKE", List.of(toMatch, regexp));
		return Conditions.isEqual(function, SQL.literalOf(true));
	}
}
