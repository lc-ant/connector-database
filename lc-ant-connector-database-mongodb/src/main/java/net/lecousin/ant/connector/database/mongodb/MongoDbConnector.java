package net.lecousin.ant.connector.database.mongodb;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.mutable.MutableObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.lecousin.ant.connector.database.DatabaseConnector;
import net.lecousin.ant.connector.database.EntityMeta;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.connector.database.annotations.Index;
import net.lecousin.ant.connector.database.exceptions.DuplicatedKeyException;
import net.lecousin.ant.connector.database.exceptions.EntityNotFoundException;
import net.lecousin.ant.connector.database.exceptions.VersionConflictException;
import net.lecousin.ant.connector.database.mongodb.mappers.MongoConverters;
import net.lecousin.ant.connector.database.request.FindRequest;
import net.lecousin.ant.connector.database.utils.PagingUtils;
import net.lecousin.ant.core.api.PageResponse;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.impl.NumberFieldReference;
import net.lecousin.ant.core.mapping.Mappers;
import net.lecousin.ant.core.patch.Patch;
import net.lecousin.ant.core.patch.PatchAppendElement;
import net.lecousin.ant.core.patch.PatchIntegerField;
import net.lecousin.ant.core.patch.PatchRemoveElement;
import net.lecousin.ant.core.patch.PatchSetField;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Slf4j
public class MongoDbConnector extends DatabaseConnector {

	private final MongoClient client;
	private final ReactiveMongoTemplate mongo;
	
	@Override
	protected <T> Mono<PageResponse<T>> doFind(FindRequest<T> request, EntityMeta meta, String collectionName) {
		Bson filter = getFilter(request, meta);
		log.debug("Search entities on collection {} with filter {}", collectionName, filter);
		
		return mongo.getCollection(collectionName)
			.flatMap(col -> {
				Mono<Optional<Long>> count;
				if (request.getPaging().isPresent() && request.getPaging().get().isWithTotal())
					count = Mono.from(col.countDocuments(filter)).map(Optional::of);
				else
					count = Mono.just(Optional.empty());

				MutableObject<FindPublisher<Document>> find = new MutableObject<>(col.find(filter));
				request.getPaging().map(PagingUtils::toPageable).filter(p -> p.isPaged()).ifPresent(paging -> {
					find.setValue(find.getValue().skip((int) paging.getOffset()).limit(paging.getPageSize()));
					if (paging.getSort() != null) {
						Document document = new Document();
						paging.getSort().forEach(order -> document.put(order.getProperty(), order.isAscending() ? 1 : -1));
						find.setValue(find.getValue().sort(document));
					}
				});

				@SuppressWarnings("unchecked")
				Mono<List<T>> data = Flux.from(find.getValue())
					.map(doc -> (T) mapDocumentToEntity(doc, meta))
					.collectList();
				return Mono.zip(data, count);
			})
			.map(tuple -> {
				log.debug("Result = {} entities", tuple.getT1().size());
				return new PageResponse<>(tuple.getT2().orElse(null), tuple.getT1());
			});
	}
	
	private <T> Bson getFilter(FindRequest<T> request, EntityMeta meta) {
		if (request.getWhere().isPresent()) {
			return getFilter(request.getEntityType(), meta, request.getWhere().get());
		}
		return new Document();
	}
	
	private <T> Bson getFilter(Class<T> entityType, EntityMeta meta, Expression<Boolean> condition) {
		CriteriaBuilder builder = new CriteriaBuilder(entityType, meta);
		var filter = builder.build(condition);
		if (filter.isEmpty()) return new Document();
		return filter.get();
	}
	
	private Object mapDocumentToEntity(Document doc, EntityMeta meta) {
		if (doc.containsKey("_id")) {
			meta.getIdProperty().ifPresent(p -> {
				doc.put(p.getName(), doc.get("_id"));
				doc.remove("_id");
			});
		}
		return Mappers.map(doc, meta.getEntityClass());
	}
	
	@Override
	protected <T> Mono<T> doCreate(T entity, EntityMeta meta, String collectionName) {
		Document doc = new Document();
		for (var prop : meta.getProperties().values()) {
			if (prop.hasAnnotation(Id.class)) {
				if (prop.hasAnnotation(GeneratedValue.class)) {
					ObjectId id = new ObjectId();
					doc.append("_id", id);
					prop.setValue(entity, Mappers.map(id, prop.getType()));
				} else {
					doc.append("_id", prop.getValue(entity));
				}
			} else if (prop.hasAnnotation(Version.class)) {
				doc.append(prop.getName(), 1L);
				prop.setValue(entity, 1L);
			} else if (meta.shouldIgnore(prop)) {
				// ignore
			} else {
				doc.append(prop.getName(), MongoConverters.toMongo(prop.getValue(entity)));
			}
		}
		log.debug("Create entity on collection {}: {}", collectionName, doc);
		return mongo.getCollection(collectionName)
			.flatMap(col -> Mono.from(col.insertOne(doc)))
			.map(r -> entity)
			.onErrorMap(MongoWriteException.class, error -> {
				if (ErrorCategory.DUPLICATE_KEY.equals(error.getError().getCategory()))
					return new DuplicatedKeyException(error.getError().getMessage());
				return error;
			});
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected <T> Mono<T> doUpdate(T entity, EntityMeta meta, String collectionName) {
		Document update = new Document();
		Document updateSet = new Document();
		update.append("$set", updateSet);
		Criteria criteriaWithVersion = new Criteria();
		Criteria criteriaWithoutVersion = new Criteria();
		boolean hasVersion = false;
		boolean idFound = false;
		for (var prop : meta.getProperties().values()) {
			if (prop.hasAnnotation(Id.class)) {
				Object id = prop.getValue(entity);
				if (prop.hasAnnotation(GeneratedValue.class))
					id = Mappers.map(id, ObjectId.class);
				criteriaWithVersion = criteriaWithVersion.and("_id").is(id);
				criteriaWithoutVersion = criteriaWithoutVersion.and("_id").is(id);
				idFound = true;
			} else if (prop.hasAnnotation(Version.class)) {
				hasVersion = true;
				Object currentValue = prop.getValue(entity);
				long currentVersion = Mappers.map(currentValue, long.class);
				criteriaWithVersion = criteriaWithVersion.and(prop.getName()).is(currentVersion);
				updateSet.append(prop.getName(), currentVersion + 1);
			} else if (meta.shouldIgnore(prop)) {
				// ignore
			} else {
				updateSet.append(prop.getName(), MongoConverters.toMongo(prop.getValue(entity)));
			}
		}
		if (!idFound) return Mono.error(new IllegalArgumentException("No property @Id on " + meta.getEntityClass().getName()));
		
		boolean hasVersionFinal = hasVersion;
		Document criteriaWithVersionDoc = criteriaWithVersion.getCriteriaObject();
		Document criteriaWithoutVersionDoc = criteriaWithoutVersion.getCriteriaObject();
		
		log.debug("Update entity on collection {} with filter {} and update request {}", collectionName, criteriaWithVersionDoc, update);

		return mongo.getCollection(collectionName)
		.flatMap(col ->
			Mono.from(col.findOneAndUpdate(criteriaWithVersionDoc, update, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)))
			.map(doc -> (T) mapDocumentToEntity(doc, meta))
			.switchIfEmpty(Mono.defer(() -> {
				if (!hasVersionFinal) return Mono.error(new EntityNotFoundException());
				return Mono.from(col.find(criteriaWithoutVersionDoc).first())
					.switchIfEmpty(Mono.error(new EntityNotFoundException()))
					.flatMap(found -> Mono.error(new VersionConflictException()));
			}))
		);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T> Mono<T> doPatchOneAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String collectionName) {
		Bson filter = getFilter(findRequest, meta);
		Document update = patchRequest(patchRequest, meta);
		
		log.debug("Patch one entity on collection {} with filter {} and update {}", collectionName, filter, update);
		
		return mongo.getCollection(collectionName)
		.flatMap(col -> Mono.from(col.findOneAndUpdate(filter, update, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER))))
		.map(doc -> (T) mapDocumentToEntity(doc, meta));
	}
	
	private Document patchRequest(Collection<Patch> patchRequest, EntityMeta meta) {
		Document doc = new Document();
		var versionOpt = meta.getVersionProperty();
		if (versionOpt.isPresent()) {
			var versionProp = versionOpt.get();
			addPatch(doc, Patch.field(new NumberFieldReference<Long>(versionProp.getName())).inc());
			patchRequest = new LinkedList<>(patchRequest);
			patchRequest.removeIf(p -> p.getFieldName().equals(versionProp.getName()));
		}
		patchRequest.forEach(patch -> addPatch(doc, patch));
		return doc;
	}
	
	private void addPatch(Document doc, Patch patch) {
		if (patch instanceof PatchSetField p) {
			((Document) doc.computeIfAbsent("$set", k -> new Document())).put(p.getFieldName(), p.getValue());
		} else if (patch instanceof PatchIntegerField p) {
			((Document) doc.computeIfAbsent("$inc", k -> new Document())).put(p.getFieldName(), p.getAddInteger());
		} else if (patch instanceof PatchAppendElement p) {
			((Document) doc.computeIfAbsent("$push", k -> new Document())).put(p.getFieldName(), p.getElementToAppend());
		} else if (patch instanceof PatchRemoveElement p) {
			((Document) doc.computeIfAbsent("$pll", k -> new Document())).put(p.getFieldName(), p.getElementToRemove());
		}
	}
	
	@Override
	protected <T> Mono<Long> doPatchManyNonAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String collectionName) {
		Bson filter = getFilter(findRequest, meta);
		Document update = patchRequest(patchRequest, meta);
		
		log.debug("Patch non-atomic many entities on collection {} with filter {} and update {}", collectionName, filter, update);
		
		return mongo.getCollection(collectionName)
		.flatMap(col -> Mono.from(col.updateMany(filter, update)))
		.map(result -> result.getModifiedCount());
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected <T> Mono<List<T>> doPatchManyAtomic(FindRequest<T> findRequest, Collection<Patch> patchRequest, EntityMeta meta, String collectionName) {
		// TODO we could use a distributed transaction, but this means mongo is well configured and in a cluster
		// else we do individual updates
		Bson filter = getFilter(findRequest, meta);
		Document update = patchRequest(patchRequest, meta);
		
		log.debug("Patch atomic many entities on collection {} with filter {} and update {}", collectionName, filter, update);
		return mongo.getCollection(collectionName)
		.flatMapMany(col ->
			Flux.from(col.find(filter))
			.flatMap(element -> col.findOneAndUpdate(filterOne(element, filter, meta), update, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)))
			.map(doc -> (T) mapDocumentToEntity(doc, meta))
		).collectList();
	}
	
	private Document filterOne(Document element, Bson filter, EntityMeta meta) {
		var idName = meta.getIdName().orElseThrow(() -> new IllegalArgumentException("No @Id property on " + meta.getEntityClass().getName()));
		var id = element.get(idName);
		return new Document("$and", List.of(new Document(idName, new Document("$eq", id)), filter));
	}

	@Override
	protected <T> Mono<Void> doDelete(EntityMeta meta, String collectionName, Expression<Boolean> where) {
		Bson filter = getFilter(meta.getEntityClass(), meta, where);
		return mongo.getCollection(collectionName)
		.flatMap(col -> Mono.from(col.deleteMany(filter)))
		.then();
	}
	
	@Override
	protected Mono<Void> createTable(EntityMeta meta, String collectionName, Index[] indexes) {
		return mongo.collectionExists(collectionName)
		.flatMap(exists -> exists ? Mono.empty() :
			mongo.createCollection(collectionName)
			.flatMap(col -> {
				List<IndexModel> indexModels = Arrays.stream(indexes)
					.map(i -> {
						StringBuilder name = new StringBuilder();
						Document keys = new Document();
						for (String field : i.fields()) {
							if (i.name().isEmpty()) {
								if (name.length() > 0) name.append('_');
								name.append(field);
							}
							keys.append(field, 1);
						}
						return new IndexModel(keys, new IndexOptions()
							.name(i.name().isEmpty() ? name.toString() : i.name())
							.unique(i.unique())
						);
					}).toList();
				return indexModels.isEmpty() ? Mono.empty() : Mono.from(col.createIndexes(indexModels));
			})
			.then()
		);
	}
	
	@Override
	protected Mono<Void> deleteTable(String tableName) {
		return mongo.dropCollection(tableName);
	}
	
	@Override
	public Mono<Void> destroy() {
		log.info("Closing MongDB Database Connector");
		return Mono.fromRunnable(client::close);
	}
}
