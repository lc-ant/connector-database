package net.lecousin.ant.connector.database.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import com.mongodb.client.model.Filters;

import lombok.AllArgsConstructor;
import lombok.Data;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.core.expression.BinaryOperationExpression;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.FieldReferenceExpression;
import net.lecousin.ant.core.expression.impl.CollectionSizeExpression;
import net.lecousin.ant.core.expression.impl.ConditionAnd;
import net.lecousin.ant.core.expression.impl.ConditionOr;
import net.lecousin.ant.core.expression.impl.GreaterOrEqualExpression;
import net.lecousin.ant.core.expression.impl.GreaterThanExpression;
import net.lecousin.ant.core.expression.impl.IsEqualExpression;
import net.lecousin.ant.core.expression.impl.IsNotEqualExpression;
import net.lecousin.ant.core.expression.impl.LessOrEqualExpression;
import net.lecousin.ant.core.expression.impl.LessThanExpression;
import net.lecousin.ant.core.expression.impl.RegexpMatchExpression;
import net.lecousin.ant.core.expression.impl.ValueExpression;
import net.lecousin.ant.core.expression.impl.ValueInExpression;
import net.lecousin.ant.core.expression.impl.ValueNotInExpression;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.reflection.ClassProperty;
import net.lecousin.ant.core.reflection.ReflectionUtils;
import net.lecousin.ant.core.reflection.ResolvedType;

@AllArgsConstructor
public class CriteriaBuilder {

	private Class<?> type;
	private EntityMeta meta;
	
	@SuppressWarnings("unchecked")
	public Optional<Bson> build(Expression<Boolean> condition) {
		if (condition instanceof ConditionAnd e) return build(e);
		if (condition instanceof ConditionOr e) return build(e);
		if (condition instanceof BinaryOperationExpression e) return build(e);
		throw new RuntimeException("Condition not supported: " + condition.getClass());
	}
	
	public Optional<Bson> build(ConditionAnd and) {
		List<Bson> list = and.getAnd().stream().map(this::build).filter(opt -> opt.isPresent()).map(opt -> opt.get()).toList();
		if (list.isEmpty()) return Optional.empty();
		return Optional.of(Filters.and(list));
	}
	
	public Optional<Bson> build(ConditionOr or) {
		List<Bson> list = or.getOr().stream().map(this::build).filter(opt -> opt.isPresent()).map(opt -> opt.get()).toList();
		if (list.isEmpty()) return Optional.empty();
		return Optional.of(Filters.or(list));
	}
	
	public Optional<Bson> build(BinaryOperationExpression<?, ?, Boolean> op) {
		if (op.leftOperand() instanceof FieldReferenceExpression fre) {
			var f = getField(fre);
			if (f.isEmpty()) return Optional.empty();
			var field = f.get();
			
			if (op.rightOperand() instanceof ValueExpression ve) {
				Object value = ve.getValue();
				
				if (op instanceof IsEqualExpression)
					return Optional.of(Filters.eq(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof IsNotEqualExpression)
					return Optional.of(Filters.ne(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof LessThanExpression)
					return Optional.of(Filters.lt(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof LessOrEqualExpression)
					return Optional.of(Filters.lte(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof GreaterThanExpression)
					return Optional.of(Filters.gt(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof GreaterOrEqualExpression)
					return Optional.of(Filters.gte(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof ValueInExpression)
					return Optional.of(Filters.in(field.getFieldName(), mapCollectionValue(value, field.getType())));
				if (op instanceof ValueNotInExpression)
					return Optional.of(Filters.nin(field.getFieldName(), mapCollectionValue(value, field.getType())));
				if (op instanceof RegexpMatchExpression)
					return Optional.of(Filters.regex(field.getFieldName(), (String) mapSingleValue(value, new ResolvedType.SingleClass(String.class))));
			}
		}
		if (op.rightOperand() instanceof FieldReferenceExpression fre) {
			var f = getField(fre);
			if (f.isEmpty()) return Optional.empty();
			var field = f.get();
			
			if (op.leftOperand() instanceof ValueExpression ve) {
				Object value = ve.getValue();
				
				if (op instanceof IsEqualExpression)
					return Optional.of(Filters.eq(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof IsNotEqualExpression)
					return Optional.of(Filters.ne(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof LessThanExpression)
					return Optional.of(Filters.gt(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof LessOrEqualExpression)
					return Optional.of(Filters.gte(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof GreaterThanExpression)
					return Optional.of(Filters.lt(field.getFieldName(), mapSingleValue(value, field.getType())));
				if (op instanceof GreaterOrEqualExpression)
					return Optional.of(Filters.lte(field.getFieldName(), mapSingleValue(value, field.getType())));
			}
		}
		
		Function<Object, Bson> operator = null;
		if (op instanceof IsEqualExpression)
			operator = expr -> new Document("$eq", expr);
		else if (op instanceof IsNotEqualExpression)
			operator = expr -> new Document("$not", new Document("$eq", expr));
		else if (op instanceof LessThanExpression)
			operator = expr -> new Document("$lt", expr);
		else if (op instanceof LessOrEqualExpression)
			operator = expr -> new Document("$lte", expr);
		else if (op instanceof GreaterThanExpression)
			operator = expr -> new Document("$gt", expr);
		else if (op instanceof GreaterOrEqualExpression)
			operator = expr -> new Document("$gte", expr);

		if (operator != null)
			return buildExpressionOperand(op.leftOperand())
			.flatMap(l -> buildExpressionOperand(op.rightOperand()).map(r -> List.of(l, r)))
			.map(operator)
			.map(e -> Filters.expr(e));
		
		throw new RuntimeException("Binary operation expression not supported betwwen " + op.leftOperand().getClass().getName() + " and " + op.rightOperand().getClass().getName());
	}
	
	private Object mapSingleValue(Object value, ResolvedType type) {
		if (value == null) return null;
		return Mappers.map(value, type);
	}
	
	private Collection<?> mapCollectionValue(Object value, ResolvedType elementType) {
		if (value == null) return List.of();
		if (value instanceof Collection c) {
			List<Object> list = new ArrayList<>(c.size());
			for (var e : c) list.add(mapSingleValue(e, elementType));
			return list;
		}
		throw new RuntimeException("Unexpected collection value: " + value.getClass().getName());
	}
	
	private Optional<Object> buildExpressionOperand(Expression<?> expression) {
		if (expression instanceof FieldReferenceExpression fre) {
			var f = getField(fre);
			if (f.isEmpty()) return Optional.empty();
			var field = f.get();
			return Optional.of("$" + field.getFieldName());
		}
		if (expression instanceof CollectionSizeExpression cse) {
			return buildExpressionOperand(cse.getSizeOfCollection()).map(c -> new Document("$size", c));
		}
		throw new RuntimeException("Operand expression not supported: " + expression.getClass().getName());
	}

	@Data
	@AllArgsConstructor
	private static class Field {
		private String fieldName;
		private ClassProperty property;
		private ResolvedType type;
	}
	
	private Optional<Field> getField(FieldReferenceExpression<?> field) {
		String fieldName = field.fieldName();
		var propertyOpt = ReflectionUtils.getClassProperty(type, fieldName);
		if (propertyOpt.isEmpty()) return Optional.empty();
		ClassProperty property = propertyOpt.get();
		if (meta.shouldIgnore(property)) return Optional.empty();
		
		ResolvedType type = property.getType();
		if (type instanceof ResolvedType.Parameterized p) {
			if (p.getBase().equals(Optional.class) ) {
				type = p.getParameters()[0];
			}
		}
		
		if (property.hasAnnotation(Id.class)) {
			fieldName = "_id";
			if (property.hasAnnotation(GeneratedValue.class)) {
				type = new ResolvedType.SingleClass(ObjectId.class);
			}
		}
		return Optional.of(new Field(fieldName, property, type));
	}
	
}
