package net.lecousin.ant.connector.database.r2dbc;

import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.r2dbc.core.DatabaseClient;

public class DeleteQuery extends SqlQuery<Delete> {


	protected DeleteQuery(DatabaseClient r2dbc, R2dbcDialect dialect, RenderContext renderContext) {
		super(r2dbc, dialect, renderContext);
	}

	@Override
	protected String render() {
		SqlRenderer renderer = renderContext != null ? SqlRenderer.create(renderContext) : SqlRenderer.create();
		return renderer.render(getQuery());
	}
}
