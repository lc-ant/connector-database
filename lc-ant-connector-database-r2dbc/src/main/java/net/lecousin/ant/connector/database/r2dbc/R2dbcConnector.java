package net.lecousin.ant.connector.database.r2dbc;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Assignment;
import org.springframework.data.relational.core.sql.AsteriskFromTable;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.DeleteBuilder.BuildDelete;
import org.springframework.data.relational.core.sql.DeleteBuilder.DeleteWhere;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder.BuildSelect;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectOrdered;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectWhere;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Update;
import org.springframework.data.relational.core.sql.UpdateBuilder.BuildUpdate;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.r2dbc.core.DatabaseClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.lecousin.ant.connector.database.DatabaseConnector;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.connector.database.exceptions.VersionConflictException;
import net.lecousin.ant.connector.database.model.EntityIndex;
import net.lecousin.ant.connector.database.r2dbc.sql.IntegerColumnAdd;
import net.lecousin.ant.connector.database.r2dbc.sql.SubselectExpression;
import net.lecousin.ant.connector.database.request.FindRequest;
import net.lecousin.ant.connector.database.utils.PagingUtils;
import net.lecousin.ant.core.api.PageRequest;
import net.lecousin.ant.core.api.PageResponse;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.impl.NumberFieldReference;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.patch.Patch;
import net.lecousin.ant.core.patch.PatchIntegerField;
import net.lecousin.ant.core.patch.PatchSetField;
import net.lecousin.ant.core.reflection.ClassProperty;
import net.lecousin.ant.core.reflection.ResolvedType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Slf4j
public abstract class R2dbcConnector extends DatabaseConnector implements InitializingBean {

	protected final DatabaseClient r2dbc;
	
	protected R2dbcDialect dialect;
	protected ExtendedR2dbcDialect extendedDialect;
	protected RenderContext renderContext;
	
	protected abstract ExtendedR2dbcDialect getExtendedDialect();
	
	@Override
	public void afterPropertiesSet() throws Exception {
		dialect = DialectResolver.getDialect(r2dbc.getConnectionFactory());
		extendedDialect = getExtendedDialect();
		renderContext = new RenderContextFactory(dialect).createRenderContext();
		super.afterPropertiesSet();
	}
	
	@Override
	protected <T> Mono<PageResponse<T>> doFind(FindRequest<T> request, EntityMeta meta, String tableName) {
		boolean needsCount = request.getPaging().map(p -> p.isWithTotal()).orElse(false);
		SelectQuery dataQuery = buildSelect(request, meta, tableName, List.of(AsteriskFromTable.create(Table.create(tableName))));
		SelectQuery countQuery = needsCount ? buildCount(request, meta, tableName) : null;
		return executeSearch(meta, dataQuery, countQuery);
	}
	
	@SuppressWarnings("unchecked")
	private <T> Mono<PageResponse<T>> executeSearch(EntityMeta meta, SelectQuery dataQuery, SelectQuery countQuery) {
		var data = dataQuery.execute().fetch().all().map(row -> Mappers.map(row, meta.getEntityClass())).collectList();
		Mono<Optional<Long>> count = countQuery != null ? countQuery.execute().fetch().one().map(row -> Optional.ofNullable((Long) row.get("count"))) : Mono.just(Optional.empty());
		
		return Mono.zip(data, count).map(tuple -> new PageResponse<T>(tuple.getT2().orElse(null), (List<T>) tuple.getT1()));
	}
	
	private <T> SelectQuery buildSelect(FindRequest<T> request, EntityMeta meta, String tableName, Collection<org.springframework.data.relational.core.sql.Expression> projection) {
		SelectQuery query = new SelectQuery(r2dbc, dialect, renderContext);
		query.setQuery(buildSelect(meta, tableName, projection, buildCondition(request, meta, tableName, query), request.getPaging()).build());
		return query;
	}
	
	private Optional<Condition> buildCondition(FindRequest<?> request, EntityMeta meta, String tableName, SqlQuery<?> query) {
		return request.getWhere().flatMap(where -> extendedDialect.createCriteriaBuilder(meta, tableName, query).build(where));
	}
	
	private BuildSelect buildSelect(
		EntityMeta meta, String tableName,
		Collection<org.springframework.data.relational.core.sql.Expression> projection,
		Optional<Condition> condition,
		Optional<PageRequest> paging
	) {
		Table table = Table.create(tableName);
		BuildSelect queryBuilder = Select.builder().select(projection).from(table);
		var optPage = paging.map(PagingUtils::toPageable);
		List<OrderByField> sort = List.of();
		if (optPage.isPresent()) {
			var page = optPage.get();
			if (page.isPaged())
				queryBuilder = ((SelectFromAndJoin) queryBuilder).limitOffset(page.getPageSize(), page.getOffset());
			sort = page.getSort().stream().map(order -> OrderByField.from(Column.create(SqlIdentifier.quoted(order.getProperty()), table), order.getDirection())).toList();
		}
		
		if (condition.isPresent())
			queryBuilder = ((SelectWhere) queryBuilder).where(condition.get());
		
		if (!sort.isEmpty())
			queryBuilder = ((SelectOrdered) queryBuilder).orderBy(sort);
		return queryBuilder;
	}
	
	private <T> SelectQuery buildCount(FindRequest<T> request, EntityMeta meta, String tableName) {
		SelectQuery countQuery = new SelectQuery(r2dbc, dialect, renderContext);
		Optional<Condition> c = request.getWhere().flatMap(where -> extendedDialect.createCriteriaBuilder(meta, tableName, countQuery).build(where));
		BuildSelect countBuilder = buildCount(tableName, c);
		countQuery.setQuery(countBuilder.build());
		return countQuery;
	}
	
	private BuildSelect buildCount(String tableName, Optional<Condition> condition) {
		Table table = Table.create(tableName);
		BuildSelect countBuilder = Select.builder().select(SimpleFunction.create("COUNT", List.of(AsteriskFromTable.create(table)))).from(table);
		if (condition.isPresent())
			countBuilder = ((SelectWhere) countBuilder).where(condition.get());
		return countBuilder;
	}
	
	@Override
	protected <T> Mono<PageResponse<T>> doTextSearch(EntityMeta meta, String tableName, EntityIndex index, String search, Optional<PageRequest> paging) {
		boolean needsCount = paging.map(p -> p.isWithTotal()).orElse(false);
		SelectQuery dataQuery = new SelectQuery(r2dbc, dialect, renderContext);
		SelectQuery countQuery = needsCount ? new SelectQuery(r2dbc, dialect, renderContext) : null;
		
		dataQuery.setQuery(
			buildSelect(
				meta, tableName, List.of(AsteriskFromTable.create(Table.create(tableName))),
				Optional.of(extendedDialect.buildTextSearchCondition(renderContext, meta, tableName, index, search, dataQuery)),
				paging
			).build()
		);
		if (countQuery != null)
			countQuery.setQuery(buildCount(tableName, Optional.of(extendedDialect.buildTextSearchCondition(renderContext, meta, tableName, index, search, countQuery))).build());
		return executeSearch(meta, dataQuery, countQuery);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T> Mono<T> doCreate(T entity, EntityMeta meta, String tableName) {
		InsertQuery query = new InsertQuery(r2dbc, dialect, renderContext);
		Table table = Table.create(tableName);
		List<Column> columns = new LinkedList<>();
		List<org.springframework.data.relational.core.sql.Expression> values = new LinkedList<>();
		List<String> generated = new LinkedList<>();
		for (var prop : meta.getProperties().values()) {
			if (prop.hasAnnotation(GeneratedValue.class)) {
				generated.add(prop.getName());
				continue;
			}
			Column col = Column.create(SqlIdentifier.quoted(prop.getName()), table);
			Object value;
			if (prop.hasAnnotation(Version.class)) {
				value = 1L;
				prop.setValue(entity, 1L);
			} else {
				value = prop.getValue(entity);
			}
			if (value instanceof Optional o)
				value = o.orElse(null);
			if (value != null)
				value = extendedDialect.encode(value);
			org.springframework.data.relational.core.sql.Expression e;
			if (value == null)
				e = SQL.nullLiteral();
			else
				e = query.marker(value);
			columns.add(col);
			values.add(e);
		}
		query.setQuery(Insert.builder().into(table).columns(columns).values(values).build());
		return query.execute()
		.filter(s -> s.returnGeneratedValues())
		.map((row, m) -> {
			for (String name : generated) {
				Object value = row.get(name);
				ClassProperty p = meta.getProperties().get(name);
				p.setValue(entity, Mappers.map(value, p.getType()));
			}
			return entity;
		}).first();
	}

	@Override
	protected <T> Mono<T> doUpdate(T entity, EntityMeta meta, String tableName) {
		UpdateQuery query = new UpdateQuery(r2dbc, dialect, renderContext);
		Table table = Table.create(tableName);
		List<Assignment> assignments = new LinkedList<>();
		org.springframework.data.relational.core.sql.Condition criteria = null;
		boolean idFound = false;
		ClassProperty versionProperty = null;
		long newVersion = 0;
		for (var prop : meta.getProperties().values()) {
			if (prop.hasAnnotation(Id.class)) {
				Object id = prop.getValue(entity);
				if (prop.hasAnnotation(GeneratedValue.class) && prop.getType() instanceof ResolvedType.SingleClass c && String.class.equals(c.getSingleClass()))
					id = UUID.fromString((String) id);
				org.springframework.data.relational.core.sql.Condition idCondition = Conditions.isEqual(Column.create(SqlIdentifier.quoted(prop.getName()), table), query.marker(id));
				criteria = criteria == null ? idCondition : criteria.and(idCondition);
				idFound = true;
			} else if (prop.hasAnnotation(Version.class)) {
				Object currentValue = prop.getValue(entity);
				long currentVersion = Mappers.map(currentValue, long.class);
				org.springframework.data.relational.core.sql.Condition versionCondition = Conditions.isEqual(Column.create(SqlIdentifier.quoted(prop.getName()), table), query.marker(currentVersion));
				criteria = criteria == null ? versionCondition : criteria.and(versionCondition);
				newVersion = currentVersion + 1;
				assignments.add(AssignValue.create(Column.create(SqlIdentifier.quoted(prop.getName()), table), query.marker(newVersion)));
				versionProperty = prop;
			} else if (meta.shouldIgnore(prop)) {
				// ignore
			} else {
				Object value = prop.getValue(entity);
				if (value != null)
					value = extendedDialect.encode(value);
				assignments.add(AssignValue.create(Column.create(SqlIdentifier.quoted(prop.getName()), table), query.marker(value)));
			}
		}
		if (!idFound) return Mono.error(new IllegalArgumentException("No property @Id on " + meta.getEntityClass().getName()));
		
		query.setQuery(Update.builder().table(table).set(assignments).where(criteria).build());
		Mono<Long> rowsUpdated = query.execute().fetch().rowsUpdated();
		if (versionProperty == null) return rowsUpdated.thenReturn(entity);
		ClassProperty vp = versionProperty;
		long nv = newVersion;
		return rowsUpdated.flatMap(updatedRows -> {
			if (updatedRows.intValue() == 0)
				return Mono.error(new VersionConflictException());
			vp.setValue(entity, nv);
			return Mono.just(entity);
		});
	}

	@Override
	protected <T> Mono<List<T>> doPatchManyAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName) {
		return getPatchMany(findRequest, patchRequest, meta, tableName, true).execute().fetch().rowsUpdated()
			.flatMap(nb -> doFind(findRequest, meta, tableName))
			.map(page -> page.getData());
	}
	
	@Override
	protected <T> Mono<Long> doPatchManyNonAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName) {
		return getPatchMany(findRequest, patchRequest, meta, tableName, false).execute().fetch().rowsUpdated();
	}
	
	@Override
	protected <T> Mono<T> doPatchOneAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName) {
		return getPatchMany(findRequest, patchRequest, meta, tableName, true).execute().fetch().rowsUpdated()
			.flatMap(nb -> doFind(findRequest, meta, tableName))
			.flatMap(page -> Mono.justOrEmpty(page.first()));
	}
	
	private <T> UpdateQuery getPatchMany(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName, boolean atomic) {
		var idName = meta.getIdName().orElseThrow(() -> new IllegalArgumentException("No @Id property on " + meta.getEntityClass().getName()));
		Table table = Table.create(tableName);
		Column idColumn = Column.create(SqlIdentifier.quoted(idName), table);
		
		UpdateQuery query = new UpdateQuery(r2dbc, dialect, renderContext);
		
		BuildSelect selectIds = buildSelect(meta, tableName, List.of(idColumn), buildCondition(findRequest, meta, tableName, query), findRequest.getPaging());
		
		List<Assignment> assignments = new LinkedList<>();
		patchRequest.forEach(p -> assignments.add(patchToAssignment(p, table, query)));
		meta.getVersionProperty().ifPresent(versionProperty -> assignments.add(patchToAssignment(Patch.field(new NumberFieldReference<Long>(versionProperty.getName())).inc(), table, query)));
		BuildUpdate update = Update.builder().table(table).set(assignments).where(Conditions.in(idColumn, getExpression(selectIds.build(), atomic)));
		
		query.setQuery(update.build());
		return query;
	}
	
	protected Assignment patchToAssignment(Patch patch, Table table, SqlQuery<?> query) {
		if (patch instanceof PatchSetField p)
			return AssignValue.create(Column.create(SqlIdentifier.quoted(p.getFieldName()), table), query.marker(p.getValue()));
		if (patch instanceof PatchIntegerField p)
			return AssignValue.create(
				Column.create(SqlIdentifier.quoted(p.getFieldName()), table),
				SQL.literalOf(new IntegerColumnAdd(Column.create(SqlIdentifier.quoted(p.getFieldName()), table), renderContext, p.getAddInteger()))
			);
		throw new RuntimeException("Patch not supported: " + patch.getClass().getSimpleName());
	}
	
	protected org.springframework.data.relational.core.sql.Expression getExpression(Select select, boolean atomic) {
		if (!atomic) return new SubselectExpression(select, renderContext);
		return new SubselectExpression(select, "FOR UPDATE", renderContext);
	}
	
	@Override
	protected <T> Mono<Void> doDelete(EntityMeta meta, String tableName, Expression<Boolean> where) {
		DeleteQuery query = new DeleteQuery(r2dbc, dialect, renderContext);
		Table table = Table.create(tableName);
		BuildDelete builder = Delete.builder().from(table);
		if (where != null) {
			Optional<Condition> c = extendedDialect.createCriteriaBuilder(meta, tableName, query).build(where);
			if (c.isPresent())
				builder = ((DeleteWhere) builder).where(c.get());
		}
		query.setQuery(builder.build());
		
		return query.execute().then();
	}

	
	@Override
	protected Mono<Void> doCreateTable(EntityMeta meta, String tableName) {
		Table table = Table.create(tableName);
		String sql = extendedDialect.createTable(table, meta.getProperties().values());
		log.info(sql);
		return r2dbc.sql(sql)
			.then()
			.thenMany(
				Flux.fromIterable(meta.getIndexes())
				.map(index -> extendedDialect.createIndex(table, index))
				.doOnNext(s -> log.info(s))
				.flatMap(s -> r2dbc.sql(s).then())
			)
			.then();
	}
	
	@Override
	protected Mono<Void> deleteTable(String tableName) {
		return r2dbc.sql("DROP TABLE " + Table.create(tableName).getName().toSql(dialect.getIdentifierProcessing())).then();
	}
	
	@Override
	public Mono<Void> destroy() {
		return Mono.fromRunnable(() -> {
			// nothing
		});
	}
	
}
