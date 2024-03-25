package net.lecousin.ant.connector.database.postgresql;

import java.util.List;
import java.util.Optional;

import org.springframework.data.relational.core.sql.SimpleFunction;

import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.r2dbc.CriteriaBuilder;
import net.lecousin.ant.connector.database.r2dbc.ExtendedR2dbcDialect;
import net.lecousin.ant.connector.database.r2dbc.SqlQuery;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.impl.CollectionSizeExpression;
import net.lecousin.ant.core.reflection.ResolvedType;

public class PostgreSQLCriteriaBuilder extends CriteriaBuilder {

	public PostgreSQLCriteriaBuilder(ExtendedR2dbcDialect dialect, EntityMeta meta, String tableName, SqlQuery<?> query) {
		super(dialect, meta, tableName, query);
	}

	@Override
	protected Optional<Operand> getSqlExpression(Expression<?> expr) {
		if (expr instanceof CollectionSizeExpression cse) {
			var col = getSqlExpression(cse.getSizeOfCollection());
			if (col.isEmpty()) return Optional.empty();
			return Optional.of(new Operand(
				SimpleFunction.create("jsonb_array_length", List.of(col.get().getExpression())),
				new ResolvedType.SingleClass(Integer.class),
				null
			));
		}
		return super.getSqlExpression(expr);
	}

}
