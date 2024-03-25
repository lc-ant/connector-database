package net.lecousin.ant.connector.database.r2dbc;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.core.expression.BinaryOperationExpression;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.FieldReferenceExpression;
import net.lecousin.ant.core.expression.impl.ConditionAnd;
import net.lecousin.ant.core.expression.impl.ConditionOr;
import net.lecousin.ant.core.expression.impl.GreaterOrEqualExpression;
import net.lecousin.ant.core.expression.impl.GreaterThanExpression;
import net.lecousin.ant.core.expression.impl.IsEqualExpression;
import net.lecousin.ant.core.expression.impl.IsNotEqualExpression;
import net.lecousin.ant.core.expression.impl.LessOrEqualExpression;
import net.lecousin.ant.core.expression.impl.LessThanExpression;
import net.lecousin.ant.core.expression.impl.ValueExpression;
import net.lecousin.ant.core.expression.impl.ValueInExpression;
import net.lecousin.ant.core.expression.impl.ValueNotInExpression;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.reflection.ResolvedType;

@RequiredArgsConstructor
public abstract class CriteriaBuilder {
	
	protected final ExtendedR2dbcDialect dialect;
	protected final EntityMeta meta;
	protected final String tableName;
	protected final SqlQuery<?> query;
	
	@SuppressWarnings("unchecked")
	public Optional<Condition> build(Expression<Boolean> condition) {
		if (condition instanceof ConditionAnd e) return build(e);
		if (condition instanceof ConditionAnd e) return build(e);
		if (condition instanceof BinaryOperationExpression e) return build(e);
		throw new RuntimeException("Condition not supported: " + condition.getClass());
	}
	
	public Optional<Condition> build(ConditionAnd and) {
		Condition result = null;
		for (var c : and.getAnd()) {
			var cd = build(c);
			if (cd.isEmpty()) continue;
			if (result == null) result = cd.get();
			else result = result.and(cd.get());
		}
		return Optional.ofNullable(result);
	}

	public Optional<Condition> build(ConditionOr or) {
		Condition result = null;
		for (var c : or.getOr()) {
			var cd = build(c);
			if (cd.isEmpty()) continue;
			if (result == null) result = cd.get();
			else result = result.or(cd.get());
		}
		return Optional.ofNullable(result);
	}
	
	@SuppressWarnings("unchecked")
	public Optional<Condition> build(BinaryOperationExpression<?, ?, Boolean> op) {
		var left = getSqlExpression(op.leftOperand());
		if (left.isEmpty()) return Optional.empty();
		var right = getSqlExpression(op.rightOperand());
		if (right.isEmpty()) return Optional.empty();
		
		var l = left.get();
		var r = right.get();
		
		if (op instanceof IsEqualExpression) return Optional.of(getConditionWithNullHandling(l, r, Conditions::isEqual, Conditions::isNull));
		if (op instanceof IsNotEqualExpression) return Optional.of(getConditionWithNullHandling(l, r, Conditions::isNotEqual, e -> Conditions.not(Conditions.isNull(e))));
		if (op instanceof LessThanExpression) return Optional.of(getCondition(l, r, Conditions::isLess));
		if (op instanceof LessOrEqualExpression) return Optional.of(getCondition(l, r, Conditions::isLessOrEqualTo));
		if (op instanceof GreaterThanExpression) return Optional.of(getCondition(l, r, Conditions::isGreater));
		if (op instanceof GreaterOrEqualExpression) return Optional.of(getCondition(l, r, Conditions::isGreaterOrEqualTo));
		if (op instanceof ValueInExpression) return Optional.of(getCollectionCondition(l, r, Conditions::in));
		if (op instanceof ValueNotInExpression) return Optional.of(getCollectionCondition(l, r, Conditions::notIn));
		throw new RuntimeException("Binary operation not supported: " + op.getClass().getName());
	}
	
	protected Condition getConditionWithNullHandling(
		Operand l, Operand r,
		BiFunction<org.springframework.data.relational.core.sql.Expression, org.springframework.data.relational.core.sql.Expression, Condition> conditionBuilder,
		Function<org.springframework.data.relational.core.sql.Expression, Condition> nullConditionBuilder) {
		if (l.expression != null) {
			if (r.expression != null)
				return conditionBuilder.apply(l.expression, r.expression);
			if (r.value == null)
				return nullConditionBuilder.apply(l.expression);
			return conditionBuilder.apply(l.expression, query.marker(mapValue(r.value, l.type, false)));
		}
		if (r.expression != null) {
			if (l.value == null)
				return nullConditionBuilder.apply(r.expression);
			return conditionBuilder.apply(r.expression, query.marker(mapValue(l.value, r.type, false)));
		}
		return conditionBuilder.apply(query.marker(l.value), query.marker(r.value));
	}
	
	protected Condition getCondition(
		Operand l, Operand r,
		BiFunction<org.springframework.data.relational.core.sql.Expression, org.springframework.data.relational.core.sql.Expression, Condition> conditionBuilder) {
		if (l.expression != null) {
			if (r.expression != null)
				return conditionBuilder.apply(l.expression, r.expression);
			return conditionBuilder.apply(l.expression, query.marker(mapValue(r.value, l.type, false)));
		}
		if (r.expression != null) {
			return conditionBuilder.apply(r.expression, query.marker(mapValue(l.value, r.type, false)));
		}
		return conditionBuilder.apply(query.marker(l.value), query.marker(r.value));
	}
	
	protected Condition getCollectionCondition(
		Operand l, Operand r,
		BiFunction<org.springframework.data.relational.core.sql.Expression, Collection<org.springframework.data.relational.core.sql.Expression>, Condition> conditionBuilder) {
		if (l.expression != null) {
			if (r.expression == null)
				return conditionBuilder.apply(l.expression, getValuesCollection(r.value, l.type));
		}
		throw new RuntimeException("Collection condition not supported with left " + l + " and right " + r);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Collection<org.springframework.data.relational.core.sql.Expression> getValuesCollection(Object value, ResolvedType type) {
		return ((Collection) mapValue(value, type, true)).stream().map(query::marker).toList();
	}
	
	protected Optional<Operand> getSqlExpression(Expression<?> expr) {
		if (expr instanceof FieldReferenceExpression fre) {
			var f = getField(fre);
			if (f.isEmpty()) return Optional.empty();
			return Optional.of(new Operand(Column.create(SqlIdentifier.quoted(f.get().getFieldName()), Table.create(tableName)), f.get().getType(), null));
		}
		if (expr instanceof ValueExpression ve) {
			return Optional.of(new Operand(null, null, ve.getValue()));
		}
		throw new RuntimeException("Expression not supported: " + expr.getClass().getName());
	}
	
	@Data
	@AllArgsConstructor
	protected static class Operand {
		private org.springframework.data.relational.core.sql.Expression expression;
		private ResolvedType type;
		private Object value;
	}
	
	@Data
	@AllArgsConstructor
	protected static class Field {
		private String fieldName;
		private ResolvedType type;
	}
	
	protected Optional<Field> getField(FieldReferenceExpression<?> field) {
		String fieldName = field.fieldName();
		
		var property = meta.getProperties().get(fieldName);
		if (property == null) return Optional.empty();
		if (meta.shouldIgnore(property)) return Optional.empty();
		
		ResolvedType type = property.getType();
		if (property.hasAnnotation(GeneratedValue.class) && type instanceof ResolvedType.SingleClass c && String.class.equals(c.getSingleClass()))
			type = new ResolvedType.SingleClass(UUID.class);
		
		return Optional.of(new Field(fieldName, type));
	}
	
	protected Object mapValue(Object value, ResolvedType type, boolean collection) {
		if (value == null) return collection ? Collections.emptyList() : null;
		if (collection) type = new ResolvedType.Parameterized(Collection.class, new ResolvedType[] { type });
		return Mappers.map(value, type);
	}
	
}
