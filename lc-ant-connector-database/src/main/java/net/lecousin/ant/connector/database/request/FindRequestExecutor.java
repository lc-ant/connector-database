package net.lecousin.ant.connector.database.request;

import java.util.Optional;
import java.util.function.Function;

import net.lecousin.ant.core.api.PageRequest;
import net.lecousin.ant.core.api.PageResponse;
import net.lecousin.ant.core.expression.Expression;
import reactor.core.publisher.Mono;

public class FindRequestExecutor<T> extends FindRequest<T> {

	private final Function<FindRequestExecutor<T>, Mono<PageResponse<T>>> executor;
	
	public FindRequestExecutor(Class<T> entityType, Function<FindRequestExecutor<T>, Mono<PageResponse<T>>> executor) {
		super(entityType);
		this.executor = executor;
	}

	public FindRequestExecutor<T> where(Optional<Expression<Boolean>> condition) {
		return (FindRequestExecutor<T>) super.where(condition);
	}

	public FindRequestExecutor<T> where(Expression<Boolean> condition) {
		return (FindRequestExecutor<T>) super.where(condition);
	}
	
	public FindRequestExecutor<T> paging(Optional<PageRequest> pageRequest) {
		return (FindRequestExecutor<T>) super.paging(pageRequest);
	}

	public FindRequestExecutor<T> paging(PageRequest pageRequest) {
		return (FindRequestExecutor<T>) super.paging(pageRequest);
	}
	
	public Mono<PageResponse<T>> execute() {
		return executor.apply(this);
	}
	
	public Mono<T> executeSingle() {
		paging(Optional.of(PageRequest.first()));
		return executor.apply(this)
			.flatMap(page -> Mono.justOrEmpty(page.first()));
	}
	
}
