package net.lecousin.ant.connector.database.r2dbc.sql;

import java.util.List;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Named;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractExpression implements Expression {

	protected final RenderContext renderContext;
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		getElementsToRender().forEach(element -> render(element, s));
		return s.toString();
	}
	
	protected abstract List<Object> getElementsToRender();
	
	protected void render(Object element, StringBuilder s) {
		if (element instanceof Column column)
			s.append(renderColumn(column));
		else if (element instanceof Named named)
			s.append(renderSqlIdentifier(named.getName()));
		else
			s.append(element.toString());
	}
	
	protected String renderColumn(Column column) {
		RenderNamingStrategy namingStrategy = renderContext.getNamingStrategy();
		return renderSqlIdentifier(SqlIdentifier.from(namingStrategy.getReferenceName(column.getTable()), namingStrategy.getName(column)));
	}
	
	protected String renderSqlIdentifier(SqlIdentifier identifier) {
		return identifier.toSql(renderContext.getIdentifierProcessing());
	}
	
}
