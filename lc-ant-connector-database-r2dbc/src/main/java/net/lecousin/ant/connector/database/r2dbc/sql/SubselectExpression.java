package net.lecousin.ant.connector.database.r2dbc.sql;

import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

public class SubselectExpression implements Expression {

	private final Select select;
	private final String suffix;
	private final RenderContext context;
	
	public SubselectExpression(Select select, String suffix, RenderContext context) {
		this.select = select;
		this.suffix = suffix;
		this.context = context;
	}

	public SubselectExpression(Select select, RenderContext context) {
		this(select, null, context);
	}
	
	@Override
	public String toString() {
		return "(" + render(select) + (suffix != null ? " " + suffix : "") + ")";
	}
	
	private String render(Select select) {
		SqlRenderer renderer = context != null ? SqlRenderer.create(context) : SqlRenderer.create();
		return renderer.render(select);
	}
}