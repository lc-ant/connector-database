package net.lecousin.ant.connector.database.request;

import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.lecousin.ant.core.api.PageRequest;
import net.lecousin.ant.core.expression.Expression;

@RequiredArgsConstructor
public class FindRequest<T> {

	@Getter
	private final Class<T> entityType;

	@Getter
	private Optional<Expression<Boolean>> where = Optional.empty();
	@Getter
	private Optional<PageRequest> paging = Optional.empty();
	
	public FindRequest<T> where(Optional<Expression<Boolean>> condition) {
		this.where = condition;
		return this;
	}

	public FindRequest<T> where(Expression<Boolean> condition) {
		return where(Optional.ofNullable(condition));
	}
	
	public FindRequest<T> paging(Optional<PageRequest> pageRequest) {
		this.paging = pageRequest;
		return this;
	}

	public FindRequest<T> paging(PageRequest pageRequest) {
		return paging(Optional.ofNullable(pageRequest));
	}
	
}
