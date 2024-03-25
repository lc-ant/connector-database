package net.lecousin.ant.connector.database.postgresql.sql;

import java.util.List;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.render.RenderContext;

import net.lecousin.ant.connector.database.r2dbc.sql.AbstractExpression;

public class JsonbArrayRemoveElement extends AbstractExpression {

	private final Column jsonbArrayColumn;
	private final Expression appendValue;

	public JsonbArrayRemoveElement(Column jsonbArrayColumn, Expression appendValue, RenderContext renderContext) {
		super(renderContext);
		this.jsonbArrayColumn = jsonbArrayColumn;
		this.appendValue = appendValue;
	}
	
	@Override
	protected List<Object> getElementsToRender() {
		return List.of(jsonbArrayColumn, " - ", appendValue);
	}
	
}
