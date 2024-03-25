package net.lecousin.ant.connector.database.postgresql.sql;

import java.util.LinkedList;
import java.util.List;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.render.RenderContext;

import net.lecousin.ant.connector.database.r2dbc.sql.AbstractExpression;

public class TextSearchCondition extends AbstractExpression {
	
	private List<Column> columns;
	private String text;

	public TextSearchCondition(RenderContext renderContext, List<Column> columns, String text) {
		super(renderContext);
		this.columns = columns;
		this.text = text;
	}
	
	@Override
	protected List<Object> getElementsToRender() {
		List<Object> elements = new LinkedList<>();
		elements.add("to_tsvector('simple', ");
		for (int i = 0; i < columns.size(); ++i) {
			if (i > 0) elements.add("|| ' ' || ");
			elements.add("coalesce(");
			elements.add(columns.get(i));
			elements.add(", '') ");
		}
		elements.add(") @@ websearch_to_tsquery('simple', ");
		elements.add(SQL.literalOf(text));
		elements.add(")");
		return elements;
	}
	
}
