package net.lecousin.ant.connector.database.r2dbc;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindTarget;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class SqlQuery<T> {

	@Getter
	@Setter
	private T query;
	private BindMarkers markers;
	private List<Pair<BindMarker, Object>> bindings = new LinkedList<>();
	protected DatabaseClient r2dbc;
	protected RenderContext renderContext;
	
	protected SqlQuery(DatabaseClient r2dbc, R2dbcDialect dialect, RenderContext renderContext) {
		this.r2dbc = r2dbc;
		this.renderContext = renderContext;
		markers = dialect.getBindMarkersFactory().create();
	}
	
	public Expression marker(Object value) {
		BindMarker marker = markers.next();
		bindings.add(Pair.of(marker, value));
		return SQL.bindMarker(marker.getPlaceholder());
	}
	
	public GenericExecuteSpec execute() {
		return r2dbc.sql(prepare());
	}
	
	public PreparedOperation<T> prepare() {
		return new PreparedOperation<>() {

			@Override
			public T getSource() {
				return query;
			}

			@Override
			public void bindTo(BindTarget target) {
				for (Pair<BindMarker, Object> binding : bindings)
					binding.getKey().bind(target, binding.getValue());
			}

			@Override
			public String toQuery() {
				String sql = render();
				log.debug(sql);
				return sql;
			}
			
		};
	}
	
	protected abstract String render();
}
