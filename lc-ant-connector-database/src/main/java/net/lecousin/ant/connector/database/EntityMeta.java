package net.lecousin.ant.connector.database;

import java.util.Map;
import java.util.Optional;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

import lombok.Getter;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.Tenant;
import net.lecousin.ant.connector.database.annotations.Tenant.TenantStrategy;
import net.lecousin.ant.core.expression.Expression;
import net.lecousin.ant.core.expression.utils.SearchFieldValueFromCondition;
import net.lecousin.ant.core.reflection.ClassProperty;
import net.lecousin.ant.core.reflection.ReflectionUtils;
import net.lecousin.ant.core.reflection.ResolvedType;

public class EntityMeta {

	@Getter
	private Class<?> entityClass;
	@Getter
	private String baseName;
	@Getter
	private Map<String, ClassProperty> properties;
	@Getter
	private Optional<TenantStrategy> tenantStrategy;
	@Getter
	private boolean autoCreate;
	
	public EntityMeta(Class<?> clazz, ApplicationContext ctx) {
		entityClass = clazz;
		Entity e = clazz.getAnnotation(Entity.class);
		baseName = e.domain();
		if (!e.name().isBlank()) baseName = baseName + "_" + e.name();
		properties = ReflectionUtils.getAllProperties(clazz);
		tenantStrategy = getTenantProperty().map(p -> {
			TenantStrategy strategy = p.getRequiredAnnotation(Tenant.class).strategy();
			if (TenantStrategy.FROM_CONFIGURATION.equals(strategy)) {
				String configuredStrategy = ctx.getEnvironment().getProperty("lc-ant.service." + e.domain() + ".tenant-strategy", TenantStrategy.MULTI_TENANT.name());
				try {
					strategy = TenantStrategy.valueOf(configuredStrategy);
				} catch (Exception error) {
					strategy = TenantStrategy.MULTI_TENANT;
				}
			}
			return strategy;
		});
		autoCreate = ctx.getEnvironment().acceptsProfiles(Profiles.of(e.autoCreateForProfiles()));
	}
	
	@SuppressWarnings("unchecked")
	public String getFinalNameFromEntity(Object entity) {
		if (tenantStrategy.isEmpty() || tenantStrategy.get().equals(TenantStrategy.MULTI_TENANT))
			return baseName;
		var tenantProperty = getTenantProperty().orElseThrow();
		Object tenantValue = tenantProperty.getValue(entity);
		if (tenantValue instanceof Optional o) tenantValue = o.orElse(null);
		if (tenantValue == null) {
			if (!Optional.class.equals(ResolvedType.getRawClass(tenantProperty.getType())))
				throw new RuntimeException("Missing tenant property");
			return baseName;
		}
		return baseName + "__" + tenantValue;
	}
	
	public String getFinalNameFromCondition(Expression<Boolean> condition) {
		if (tenantStrategy.isEmpty() || tenantStrategy.get().equals(TenantStrategy.MULTI_TENANT))
			return baseName;
		var tenantProperty = getTenantProperty().orElseThrow();
		var optTenant = SearchFieldValueFromCondition.searchFieldValue(tenantProperty.getName(), condition);
		if (optTenant.isEmpty() || optTenant.get() == null) {
			if (!Optional.class.equals(ResolvedType.getRawClass(tenantProperty.getType()).orElse(null)))
				throw new RuntimeException("Missing tenant property");
			return baseName;
		}
		return baseName + "__" + optTenant.get();
	}
	
	public Optional<ClassProperty> getIdProperty() {
		return properties.values().stream().filter(p -> p.hasAnnotation(Id.class)).findAny();
	}
	
	public Optional<ClassProperty> getVersionProperty() {
		return properties.values().stream().filter(p -> p.hasAnnotation(Version.class)).findAny();
	}
	
	public Optional<String> getIdName() {
		return getIdProperty().map(ClassProperty::getName);
	}
	
	public Optional<ClassProperty> getTenantProperty() {
		return properties.values().stream().filter(p -> p.hasAnnotation(Tenant.class)).findAny();
	}
	
	public boolean shouldIgnore(ClassProperty property) {
		if (property.hasAnnotation(Tenant.class) && TenantStrategy.SEPARATED_TENANT.equals(tenantStrategy.orElse(null)))
			return true;
		return false;
	}
	
}
