package net.lecousin.ant.connector.database.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;

import net.lecousin.ant.connector.database.DatabaseConnector;
import net.lecousin.ant.core.api.ApiData;
import net.lecousin.ant.core.api.PageRequest;
import net.lecousin.ant.core.patch.Patch;
import net.lecousin.ant.core.springboot.connector.ConnectorService;

@SpringBootTest(classes = DatabaseConnectorTestConfig.class, webEnvironment = WebEnvironment.NONE)
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractDatabaseConnectorTest {

	@Autowired private ConnectorService connectorService;
	
	protected DatabaseConnector db;
	
	protected abstract String getImplementationName();
	
	protected abstract Map<String, Object> getProperties();
	
	@BeforeEach
	void setUp() {
		db = connectorService.getConnector(DatabaseConnector.class, getImplementationName(), "test", getProperties()).block();
	}
	
	@Test
	@Order(1)
	void createFindDeleteSimpleEntity() {
		var entity = db.create(SimpleEntity.builder().integer(1234).build()).block();
		assertThat(entity).isNotNull();
		assertThat(entity.getInteger()).isEqualTo(1234);
		assertThat(entity.getId()).isNotBlank();
		
		var page = db.find(SimpleEntity.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(entity);

		var entity2 = db.create(SimpleEntity.builder().integer(4321).build()).block();
		assertThat(entity2).isNotNull();
		assertThat(entity2.getInteger()).isEqualTo(4321);
		assertThat(entity2.getId()).isNotBlank();

		page = db.find(SimpleEntity.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(2).containsExactlyInAnyOrder(entity, entity2);
		assertThat(page.getTotal()).isNull();
		
		page = db.find(SimpleEntity.class).paging(PageRequest.builder().page(0).pageSize(10).withTotal(true).build()).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(2).containsExactlyInAnyOrder(entity, entity2);
		assertThat(page.getTotal()).isEqualTo(2L);
		
		var e = db.findById(SimpleEntity.class, entity.getId()).block();
		assertThat(e).isEqualTo(entity);
		
		e = db.findById(SimpleEntity.class, entity2.getId()).block();
		assertThat(e).isEqualTo(entity2);
		
		page = db.find(SimpleEntity.class).where(ApiData.FIELD_ID.in(List.of(entity.getId()))).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactlyInAnyOrder(entity);
		page = db.find(SimpleEntity.class).where(ApiData.FIELD_ID.in(List.of(entity2.getId()))).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactlyInAnyOrder(entity2);
		page = db.find(SimpleEntity.class).where(ApiData.FIELD_ID.in(List.of(entity.getId(), entity2.getId()))).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(2).containsExactlyInAnyOrder(entity, entity2);

		
		db.delete(SimpleEntity.class, ApiData.FIELD_ID.is(entity.getId())).block();
		
		page = db.find(SimpleEntity.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(entity2);
		
		db.delete(SimpleEntity.class, ApiData.FIELD_ID.is(entity2.getId())).block();
		
		page = db.find(SimpleEntity.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).isEmpty();
	}
	
	@Test
	@Order(10)
	void createFindDeleteEntityWithNumbers() {
		var entity = EntityWithNumbers.builder()
			.b((byte) 1)
			.bo(Byte.valueOf((byte) 2))
			.s((short) 3)
			.so(Short.valueOf((short) 4))
			.i(5)
			.io(6)
			.l(7L)
			.lo(8L)
			.f(9.12f)
			.fo(Float.valueOf(10.23f))
			.d(11.45d)
			.dob(Double.valueOf(12.67d))
			.build();
		entity = db.create(entity).block();
		assertThat(entity).isNotNull();
		assertThat(entity.getId()).isNotBlank();
		assertThat(entity.getB()).isEqualTo((byte) 1);
		assertThat(entity.getBo()).isEqualTo((byte) 2);
		assertThat(entity.getS()).isEqualTo((short) 3);
		assertThat(entity.getSo()).isEqualTo((short) 4);
		assertThat(entity.getI()).isEqualTo(5);
		assertThat(entity.getIo()).isEqualTo(6);
		assertThat(entity.getL()).isEqualTo(7L);
		assertThat(entity.getLo()).isEqualTo(8L);
		assertThat(entity.getF()).isEqualTo(9.12f);
		assertThat(entity.getFo()).isEqualTo(10.23f);
		assertThat(entity.getD()).isEqualTo(11.45d);
		assertThat(entity.getDob()).isEqualTo(12.67d);
		
		var page = db.find(EntityWithNumbers.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(entity);
		
		var entity2 = EntityWithNumbers.builder()
			.b((byte) 1)
			.s((short) 3)
			.i(5)
			.l(7L)
			.f(9.12f)
			.d(11.45d)
			.build();
		entity2 = db.create(entity2).block();
		assertThat(entity2).isNotNull();
		assertThat(entity2.getId()).isNotBlank();
		assertThat(entity2.getB()).isEqualTo((byte) 1);
		assertThat(entity2.getBo()).isNull();
		assertThat(entity2.getS()).isEqualTo((short) 3);
		assertThat(entity2.getSo()).isNull();
		assertThat(entity2.getI()).isEqualTo(5);
		assertThat(entity2.getIo()).isNull();
		assertThat(entity2.getL()).isEqualTo(7L);
		assertThat(entity2.getLo()).isNull();
		assertThat(entity2.getF()).isEqualTo(9.12f);
		assertThat(entity2.getFo()).isNull();
		assertThat(entity2.getD()).isEqualTo(11.45d);
		assertThat(entity2.getDob()).isNull();
		
		page = db.find(EntityWithNumbers.class).paging(PageRequest.builder().page(0).pageSize(10).withTotal(true).build()).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(2).containsExactlyInAnyOrder(entity, entity2);
		assertThat(page.getTotal()).isEqualTo(2L);
		
		var e = db.findById(EntityWithNumbers.class, entity.getId()).block();
		assertThat(e).isEqualTo(entity);
		
		e = db.findById(EntityWithNumbers.class, entity2.getId()).block();
		assertThat(e).isEqualTo(entity2);
		
		db.delete(EntityWithNumbers.class, ApiData.FIELD_ID.is(entity.getId())).block();
		
		page = db.find(EntityWithNumbers.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(entity2);
		
		db.delete(EntityWithNumbers.class, ApiData.FIELD_ID.is(entity2.getId())).block();
		
		page = db.find(EntityWithNumbers.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).isEmpty();
	}
	
	@Test
	@Order(50)
	void createFindDeleteEntityWithTemporals() {
		long timestamp = System.currentTimeMillis();
		var entity = EntityWithTemporals.builder()
			.instant(Instant.ofEpochMilli(timestamp))
			.localDate(LocalDate.now())
			.localTime(LocalTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()))
			.localDateTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()))
			.build();
		var created = db.create(entity).block();
		assertThat(created).isNotNull();
		assertThat(created.getId()).isNotBlank();
		assertThat(created.getInstant()).isEqualTo(entity.getInstant());
		assertThat(created.getLocalDate()).isEqualTo(entity.getLocalDate());
		assertThat(created.getLocalTime()).isEqualTo(entity.getLocalTime());
		assertThat(created.getLocalDateTime()).isEqualTo(entity.getLocalDateTime());
		
		var page = db.find(EntityWithTemporals.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(created);
		
		db.delete(EntityWithTemporals.class, ApiData.FIELD_ID.is(entity.getId())).block();
		
		page = db.find(EntityWithTemporals.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).isEmpty();
	}

	
	@Test
	@Order(100)
	void createFindDeleteEntityWithOptional() {
		var entity = EntityWithOptional.builder()
			.str(Optional.of("test"))
			.build();
		var created = db.create(entity).block();
		assertThat(created).isNotNull();
		assertThat(created.getId()).isNotBlank();
		assertThat(created.getStr()).isPresent().contains("test");
		
		var page = db.find(EntityWithOptional.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(created);
		
		var entity2 = EntityWithOptional.builder()
			.str(Optional.empty())
			.build();
		entity2 = db.create(entity2).block();
		assertThat(entity2).isNotNull();
		assertThat(entity2.getId()).isNotBlank();
		assertThat(entity2.getStr()).isEmpty();
		
		page = db.find(EntityWithOptional.class).paging(PageRequest.builder().page(0).pageSize(10).withTotal(true).build()).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(2).containsExactlyInAnyOrder(created, entity2);
		assertThat(page.getTotal()).isEqualTo(2L);
		
		var e = db.findById(EntityWithOptional.class, entity.getId()).block();
		assertThat(e).isEqualTo(created);
		
		e = db.findById(EntityWithOptional.class, entity2.getId()).block();
		assertThat(e).isEqualTo(entity2);
		
		db.delete(EntityWithOptional.class, ApiData.FIELD_ID.is(entity.getId())).block();
		
		page = db.find(EntityWithOptional.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(entity2);
		
		db.delete(EntityWithOptional.class, ApiData.FIELD_ID.is(entity2.getId())).block();
		
		page = db.find(EntityWithOptional.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).isEmpty();
	}
	
	@Test
	@Order(1000)
	void createFindDeleteWithList() {
		var entity = EntityWithList.builder()
			.values(List.of("one", "two"))
			.build();
		var created = db.create(entity).block();
		assertThat(created).isNotNull();
		assertThat(created.getId()).isNotBlank();
		assertThat(created.getValues()).containsExactly("one", "two");
		
		var page = db.find(EntityWithList.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(created);
		
		var entity2 = EntityWithList.builder()
			.values(List.of("abc", "def", "ghi"))
			.build();
		entity2 = db.create(entity2).block();
		assertThat(entity2).isNotNull();
		assertThat(entity2.getId()).isNotBlank();
		assertThat(entity2.getValues()).containsExactly("abc", "def", "ghi");
		
		page = db.find(EntityWithList.class).paging(PageRequest.builder().page(0).pageSize(10).withTotal(true).build()).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(2).containsExactlyInAnyOrder(created, entity2);
		assertThat(page.getTotal()).isEqualTo(2L);
		
		var e = db.findById(EntityWithList.class, entity.getId()).block();
		assertThat(e).isEqualTo(created);
		
		e = db.findById(EntityWithList.class, entity2.getId()).block();
		assertThat(e).isEqualTo(entity2);
		
		db.delete(EntityWithList.class, ApiData.FIELD_ID.is(entity.getId())).block();
		
		page = db.find(EntityWithList.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).hasSize(1).containsExactly(entity2);
		
		db.delete(EntityWithList.class, ApiData.FIELD_ID.is(entity2.getId())).block();
		
		page = db.find(EntityWithList.class).execute().block();
		assertThat(page).isNotNull();
		assertThat(page.getData()).isEmpty();
	}
	
	@Test
	@Order(10000)
	void conditionCompareFields() {
		var entity1 = EntityWithNumbers.builder().s((short) 10).i(20).l(30L).build();
		var entity2 = EntityWithNumbers.builder().s((short) 30).i(20).l(10L).build();
		var entity3 = EntityWithNumbers.builder().s((short) 10).i(10).l(10L).build();
		var list = db.create(List.of(entity1, entity2, entity3)).collectList().block();
		entity1 = list.get(0);
		entity2 = list.get(1);
		entity3 = list.get(2);
		
		var page = db.find(EntityWithNumbers.class).where(EntityWithNumbers.FIELD_S.lessThan(EntityWithNumbers.FIELD_I)).execute().block();
		assertThat(page.getData()).containsExactly(entity1);
		
		page = db.find(EntityWithNumbers.class).where(EntityWithNumbers.FIELD_S.lessOrEqualTo(EntityWithNumbers.FIELD_I)).execute().block();
		assertThat(page.getData()).containsExactlyInAnyOrder(entity1, entity3);
		
		page = db.find(EntityWithNumbers.class).where(EntityWithNumbers.FIELD_S.greaterThan(EntityWithNumbers.FIELD_I)).execute().block();
		assertThat(page.getData()).containsExactly(entity2);
		
		page = db.find(EntityWithNumbers.class).where(EntityWithNumbers.FIELD_S.greaterOrEqualTo(EntityWithNumbers.FIELD_I)).execute().block();
		assertThat(page.getData()).containsExactlyInAnyOrder(entity2, entity3);
	}
	
	@Test
	@Order(10100)
	void conditionOnCollectionSize() {
		var entity1 = EntityWithList.builder().i(0).values(List.of()).build();
		var entity2 = EntityWithList.builder().i(0).values(List.of("one")).build();
		var entity3 = EntityWithList.builder().i(1).values(List.of("one")).build();
		var entity4 = EntityWithList.builder().i(1).values(List.of("one", "two")).build();
		var entity5 = EntityWithList.builder().i(2).values(List.of("one", "two")).build();
		var list = db.create(List.of(entity1, entity2, entity3, entity4, entity5)).collectList().block();
		entity1 = list.get(0);
		entity2 = list.get(1);
		entity3 = list.get(2);
		entity4 = list.get(3);
		entity5 = list.get(4);
		
		var page = db.find(EntityWithList.class).where(EntityWithList.FIELD_I.is(EntityWithList.FIELD_VALUES.size())).execute().block();
		assertThat(page.getData()).containsExactlyInAnyOrder(entity1, entity3, entity5);
		
		page = db.find(EntityWithList.class).where(EntityWithList.FIELD_I.greaterThan(EntityWithList.FIELD_VALUES.size())).execute().block();
		assertThat(page.getData()).isEmpty();
		
		page = db.find(EntityWithList.class).where(EntityWithList.FIELD_I.lessThan(EntityWithList.FIELD_VALUES.size())).execute().block();
		assertThat(page.getData()).containsExactlyInAnyOrder(entity2, entity4);
	}
	
	
	@Test
	@Order(20001)
	void updateSimpleEntity() {
		var entity1 = db.create(SimpleEntity.builder().integer(1234).build()).block();
		var entity2 = db.create(SimpleEntity.builder().integer(9876).build()).block();
		
		entity1.setInteger(4321);
		entity1 = db.update(entity1).block();
		var page = db.find(SimpleEntity.class).execute().block();
		assertThat(page.getData()).containsExactlyInAnyOrder(entity1, entity2);
		entity1 = db.findById(SimpleEntity.class, entity1.getId()).block();
		assertThat(entity1.getInteger()).isEqualTo(4321);
		entity2 = db.findById(SimpleEntity.class, entity2.getId()).block();
		assertThat(entity2.getInteger()).isEqualTo(9876);
		
		db.delete(entity1, entity2).block();
	}
	
	@Test
	@Order(20002)
	void patchSimpleEntity() {
		var entity1 = db.create(SimpleEntity.builder().integer(1234).build()).block();
		var entity2 = db.create(SimpleEntity.builder().integer(9876).build()).block();

		var patched = db.patchOne(entity1, List.of(Patch.field(SimpleEntity.FIELD_INTEGER).set(12345))).block();
		assertThat(patched.getInteger()).isEqualTo(12345);
		var page = db.find(SimpleEntity.class).execute().block();
		assertThat(page.getData()).containsExactlyInAnyOrder(patched, entity2);
		
		entity1 = db.findById(SimpleEntity.class, entity1.getId()).block();
		assertThat(entity1.getInteger()).isEqualTo(12345);
		
		entity2 = db.findById(SimpleEntity.class, entity2.getId()).block();
		assertThat(entity2.getInteger()).isEqualTo(9876);
		
		db.delete(entity1, entity2).block();
	}

	
	@Test
	@Order(21000)
	void updateList() {
		var entity1 = db.create(EntityWithList.builder().values(List.of("one", "two")).build()).block();
		var entity2 = db.create(EntityWithList.builder().values(List.of("un", "deux")).build()).block();
		
		var values = new LinkedList<>(entity1.getValues());
		values.add("three");
		entity1.setValues(values);
		entity1 = db.update(entity1).block();
		assertThat(entity1.getValues()).containsExactly("one", "two", "three");

		entity1 = db.findById(EntityWithList.class, entity1.getId()).block();
		assertThat(entity1.getValues()).containsExactly("one", "two", "three");

		entity2 = db.findById(EntityWithList.class, entity2.getId()).block();
		assertThat(entity2.getValues()).containsExactly("un", "deux");
		
		db.delete(entity1, entity2).block();
	}

	
	@Test
	@Order(21001)
	void patchAppendElement() {
		var entity1 = db.create(EntityWithList.builder().values(List.of("one", "two")).build()).block();
		var entity2 = db.create(EntityWithList.builder().values(List.of("un", "deux")).build()).block();
		
		entity1 = db.patchOne(entity1, List.of(Patch.field(EntityWithList.FIELD_VALUES).appendElement("three"))).block();
		assertThat(entity1.getValues()).containsExactly("one", "two", "three");

		entity1 = db.findById(EntityWithList.class, entity1.getId()).block();
		assertThat(entity1.getValues()).containsExactly("one", "two", "three");

		entity2 = db.findById(EntityWithList.class, entity2.getId()).block();
		assertThat(entity2.getValues()).containsExactly("un", "deux");
		
		db.delete(entity1, entity2).block();
	}

}
