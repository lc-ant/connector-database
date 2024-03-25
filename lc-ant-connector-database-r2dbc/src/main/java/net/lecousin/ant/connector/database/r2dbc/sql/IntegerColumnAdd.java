package net.lecousin.ant.connector.database.r2dbc.sql;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class IntegerColumnAdd {

	private final Column column;
	private final RenderContext context;
	private final long add;
	
	@Override
	public String toString() {
		RenderNamingStrategy namingStrategy = context.getNamingStrategy();
		TableLike table = column.getTable();
		SqlIdentifier columnIdentifier = SqlIdentifier.from(namingStrategy.getReferenceName(table), namingStrategy.getReferenceName(column));
		return columnIdentifier.toSql(context.getIdentifierProcessing()) + " + " + add;
	}
	
}
