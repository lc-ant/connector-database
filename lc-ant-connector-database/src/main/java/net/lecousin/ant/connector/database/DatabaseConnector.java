package net.lecousin.ant.connector.database;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.SimpleMetadataReaderFactory;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.Index;
import net.lecousin.ant.connector.database.annotations.Tenant.TenantStrategy;
import net.lecousin.ant.connector.database.request.FindRequest;
import net.lecousin.ant.connector.database.request.FindRequestExecutor;
import net.lecousin.ant.core.api.PageRequest;
import net.lecousin.ant.core.api.PageResponse;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.impl.ConditionAnd;
import net.lecousin.ant.core.expression.impl.FieldReference;
import net.lecousin.ant.core.expression.impl.IsEqualExpression;
import net.lecousin.ant.core.expression.impl.ValueExpression;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.patch.Patch;
import net.lecousin.ant.core.springboot.connector.Connector;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public abstract class DatabaseConnector implements Connector, ApplicationContextAware, InitializingBean {

	protected static final Map<Class<?>, EntityMeta> META = new HashMap<>();
	@Setter
	protected ApplicationContext applicationContext;
	
	@Override
	public void afterPropertiesSet() throws Exception {
		synchronized (META) {
			if (META.isEmpty()) loadMeta(applicationContext);
		}
	}
	
	private static void loadMeta(ApplicationContext applicationContext) {
		long startTime = System.currentTimeMillis();
		PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
		Resource[] classResources;
		try {
			classResources = resourceResolver.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + "**/*.class");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		SimpleMetadataReaderFactory metadataReaderFactory = new SimpleMetadataReaderFactory();
		String annotationName = Entity.class.getName();
		for (Resource classResource : classResources) {
			try {
				MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(classResource);
				AnnotationMetadata annotationMetadata = metadataReader.getAnnotationMetadata();
				if (annotationMetadata.hasAnnotation(annotationName)) {
					Class<?> clazz = Class.forName(annotationMetadata.getClassName());
					try {
						META.put(clazz, new EntityMeta(clazz, applicationContext));
					} catch (Exception e) {
						log.error("Error analyzing entity {}", clazz.getName(), e);
					}
				}
			} catch (@SuppressWarnings("java:S1181") Throwable t) {
				// ignore
			}
		}
		log.info("{} entity classes found in {} ms.", META.size(), System.currentTimeMillis() - startTime);
	}
	
	public static EntityMeta getMetadata(Class<?> entityType, ApplicationContext context) {
		synchronized (META) {
			if (META.isEmpty()) loadMeta(context);
		}
		return META.get(entityType);
	}
	
	private Map<String, Mono<Void>> autoCreateDone = new HashMap<>();
	
	private Mono<Pair<EntityMeta, String>> checkAutoCreate(Class<?> entityType, Object entity) {
		return checkAutoCreate(entityType, (EntityMeta meta) -> meta.getFinalNameFromEntity(entity));
	}
	
	private Mono<Pair<EntityMeta, String>> checkAutoCreate(Class<?> entityType, Expression<Boolean> condition) {
		return checkAutoCreate(entityType, (EntityMeta meta) -> meta.getFinalNameFromCondition(condition));
	}
	
	private Mono<Pair<EntityMeta, String>> checkAutoCreate(Class<?> entityType, Function<EntityMeta, String> tableNameSupplier) {
		return Mono.defer(() -> {
			EntityMeta meta = META.get(entityType);
			if (meta == null) return Mono.error(new IllegalArgumentException("Unknown entity " + entityType.getName()));
			String tableName = tableNameSupplier.apply(meta);
			Mono<Void> mono;
			synchronized (autoCreateDone) {
				mono = autoCreateDone.computeIfAbsent(tableName, k -> createTable(meta, tableName).checkpoint("Create database table " + tableName).cache());
			}
			return mono.thenReturn(Pair.of(meta, tableName));
		});
	}
	
	private Mono<Void> createTable(EntityMeta meta, String tableName) {
		// TODO exclude @Tenant if separated tables
		Index[] indexes = meta.getEntityClass().getAnnotationsByType(Index.class);
		return createTable(meta, tableName, indexes);
	}

	protected abstract Mono<Void> createTable(EntityMeta meta, String finalName, Index[] indexes);

	
	public final <T> FindRequestExecutor<T> find(Class<T> entityType) {
		return new FindRequestExecutor<>(entityType, this::find);
	}
	
	public final <T> Mono<T> findById(Class<T> entityType, Serializable id) {
		Objects.requireNonNull(entityType, "entityType");
		Objects.requireNonNull(id, "id");
		return Mono.fromCallable(() -> META.get(entityType))
			.switchIfEmpty(Mono.defer(() -> Mono.error(new IllegalStateException("Unknown entity " + entityType.getName()))))
			.flatMap(meta -> Mono.justOrEmpty(meta.getIdName()))
			.switchIfEmpty(Mono.defer(() -> Mono.error(new IllegalStateException("No @Id found on entity " + entityType.getName()))))
			.flatMap(idName -> 
				find(entityType)
				.where(new IsEqualExpression<>(new FieldReference<Serializable>("id"), new ValueExpression<>(id)))
				.executeSingle()
			);
	}
	
	public final <T> Mono<PageResponse<T>> find(FindRequest<T> request) {
		return checkAutoCreate(request.getEntityType(), request.getWhere().orElse(null))
			.flatMap(meta -> doFind(request, meta.getKey(), meta.getValue()))
			.checkpoint("Database find " + request.getEntityType().getName());
	}
	
	public final <T> Mono<T> findOne(Class<T> entityType, Expression<Boolean> condition) {
		Objects.requireNonNull(entityType, "entityType");
		return find(entityType).where(condition).paging(PageRequest.of(1))
		.executeSingle();
	}

	protected abstract <T> Mono<PageResponse<T>> doFind(FindRequest<T> request, EntityMeta meta, String tableName);
	
	public final <T> Mono<T> create(T entity) {
		Objects.requireNonNull(entity, "entity");
		return checkAutoCreate(entity.getClass(), entity)
			.flatMap(meta -> doCreate(entity, meta.getKey(), meta.getValue()))
			.checkpoint("Database create entity " + entity);
	}
	
	public final <T> Flux<T> create(Collection<T> entities) {
		if (entities == null || entities.isEmpty()) return Flux.empty();
		// TODO create multiple at once, but we need to group by tenant if necessary
		return Flux.fromIterable(entities)
			.flatMapSequential(this::create);
	}

	protected abstract <T> Mono<T> doCreate(T entity, EntityMeta meta, String tableName);
	
	public final <I, T> Mono<T> update(I input, Class<T> entityType) {
		Objects.requireNonNull(input, "input");
		Objects.requireNonNull(entityType, "entityType");
		return update(Mappers.map(input, entityType));
	}
	
	public final <T> Mono<T> update(T entity) {
		Objects.requireNonNull(entity, "entity");
		return checkAutoCreate(entity.getClass(), entity)
			.flatMap(meta -> doUpdate(entity, meta.getKey(), meta.getValue()))
			.checkpoint("Database update entity " + entity);
	}

	protected abstract <T> Mono<T> doUpdate(T entity, EntityMeta meta, String tableName);
	
	public final <T> Mono<T> save(T entity) {
		Objects.requireNonNull(entity, "entity");
		return checkAutoCreate(entity.getClass(), entity)
			.flatMap(meta -> {
				Object id = meta.getKey().getIdProperty()
				.orElseThrow()
				.getValue(entity);
				if (id == null) return doCreate(entity, meta.getKey(), meta.getValue());
				return doUpdate(entity, meta.getKey(), meta.getValue());
			});
	}
	
	public final <T> Mono<List<T>> patchManyAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest) {
		if (findRequest.getPaging().map(PageRequest::getPageSize).orElse(0) == 1)
			return patchOne(findRequest, patchRequest).map(List::of);
		return checkAutoCreate(findRequest.getEntityType(), findRequest.getWhere().orElse(null))
			.flatMap(meta -> doPatchManyAtomic(findRequest, patchRequest, meta.getKey(), meta.getValue()))
			.checkpoint("Database patchManyAtomic " + findRequest.getEntityType().getName());
	}
	
	public final <T> Mono<Long> patchManyNonAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest) {
		return checkAutoCreate(findRequest.getEntityType(), findRequest.getWhere().orElse(null))
			.flatMap(meta -> doPatchManyNonAtomic(findRequest, patchRequest, meta.getKey(), meta.getValue()))
			.checkpoint("Database patchManyNonAtomic " + findRequest.getEntityType().getName());
	}
	
	public final <T> Mono<T> patchOne(FindRequest<T> findRequest, Collection<Patch> patchRequest) {
		if (findRequest.getPaging().isEmpty())
			findRequest.paging(PageRequest.first());
		else
			findRequest.getPaging().get().forcePaging(0, 1);
		return checkAutoCreate(findRequest.getEntityType(), findRequest.getWhere().orElse(null))
			.flatMap(meta -> doPatchOneAtomic(findRequest, patchRequest, meta.getKey(), meta.getValue()))
			.checkpoint("Database patchOne " + findRequest.getEntityType().getName());
	}
	
	public final <T> Mono<T> patchOne(T entity, Expression<Boolean> condition, Collection<Patch> patchRequest) {
		@SuppressWarnings("unchecked")
		Class<T> entityType = (Class<T>) entity.getClass();
		EntityMeta meta = META.get(entityType);
		if (meta == null) return Mono.error(new IllegalArgumentException("Unknown entity " + entityType.getName()));
		var idOpt = meta.getIdProperty();
		if (idOpt.isEmpty()) return Mono.error(new IllegalArgumentException("Entity does not have an @Id property: " + entityType.getName()));
		var idProp = idOpt.get();
		Expression<Boolean> idCondition = new IsEqualExpression<>(new FieldReference<Serializable>(idProp.getName()), new ValueExpression<>((Serializable) idProp.getValue(entity)));
		Expression<Boolean> cd = condition == null ? idCondition : new ConditionAnd(idCondition, condition);
		return patchOne(new FindRequest<>(entityType).where(cd), patchRequest);
	}
	
	public final <T> Mono<T> patchOne(T entity, Collection<Patch> patchRequest) {
		return patchOne(entity, null, patchRequest);
	}
	
	public final <T> Mono<T> patchOneById(Class<T> entityType, Serializable id, Collection<Patch> patchRequest) {
		EntityMeta meta = META.get(entityType);
		if (meta == null) return Mono.error(new IllegalArgumentException("Unknown entity " + entityType.getName()));
		var idOpt = meta.getIdProperty();
		if (idOpt.isEmpty()) return Mono.error(new IllegalArgumentException("Entity does not have an @Id property: " + entityType.getName()));
		var idProp = idOpt.get();
		Expression<Boolean> idCondition = new IsEqualExpression<>(new FieldReference<Serializable>(idProp.getName()), new ValueExpression<>(id));
		return patchOne(new FindRequest<>(entityType).where(idCondition), patchRequest);
	}
	
	protected abstract <T> Mono<T> doPatchOneAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName);
	
	protected abstract <T> Mono<List<T>> doPatchManyAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName);

	protected abstract <T> Mono<Long> doPatchManyNonAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String tableName);
	
	public final <T> Mono<Void> delete(Class<T> entityType, Expression<Boolean> where) {
		Objects.requireNonNull(entityType, "entityType");
		return checkAutoCreate(entityType, where)
			.flatMap(meta -> doDelete(meta.getKey(), meta.getValue(), where))
			.checkpoint("Database delete entity " + entityType.getName());
	}

	public final <T> Mono<Void> delete(T entity) {
		return checkAutoCreate(entity.getClass(), entity)
		.flatMap(meta -> {
			var idOpt = meta.getKey().getIdProperty();
			if (idOpt.isPresent()) {
				var idProp = idOpt.get();
				Expression<Boolean> condition = new IsEqualExpression<>(new FieldReference<Serializable>(idProp.getName()), new ValueExpression<>((Serializable) idProp.getValue(entity)));
				return doDelete(meta.getKey(), meta.getValue(), condition);
			} else {
				// TODO
				return Mono.error(new RuntimeException("TODO"));
			}
		});
	}

	@SafeVarargs
	public final <T> Mono<Void> delete(T... entities) {
		// TODO group by tenant if necessary...
		return Flux.fromArray(entities)
		.flatMap(this::delete)
		.then();
	}

	public final <T> Mono<Void> delete(Iterable<T> entities) {
		// TODO group by tenant if necessary...
		return Flux.fromIterable(entities)
		.flatMap(this::delete)
		.then();
	}
	
	public final <T> Mono<Void> delete(Stream<T> entities) {
		// TODO group by tenant if necessary...
		return Flux.fromStream(entities)
		.flatMap(this::delete)
		.then();
	}
	
	protected abstract <T> Mono<Void> doDelete(EntityMeta meta, String tableName, Expression<Boolean> where);
	
	public final <T> Mono<Void> deleteTenantTable(Class<T> entityType, String tenantId) {
		EntityMeta meta = META.get(entityType);
		if (meta == null) return Mono.error(new IllegalArgumentException("Unknown entity " + entityType.getName()));
		if (meta.getTenantStrategy().isEmpty() || meta.getTenantStrategy().get().equals(TenantStrategy.MULTI_TENANT))
			return Mono.empty();
		String tableName = meta.getBaseName() + "__" + tenantId;
		synchronized (autoCreateDone) {
			autoCreateDone.remove(tableName);
		}
		return deleteTable(tableName).checkpoint("Delete table " + tableName + " for tenant " + tenantId);
	}
	
	protected abstract Mono<Void> deleteTable(String tableName);
	
}
