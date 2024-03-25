package net.lecousin.ant.connector.database.utils;

import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.lecousin.ant.core.api.PageRequest;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PagingUtils {

	public static Pageable toPageable(PageRequest page) {
		if (page.getPageSize() == null) {
			if (page.getSort() == null || page.getSort().isEmpty())
				return Pageable.unpaged();
			return Pageable.unpaged(Sort.by(page.getSort().stream().map(PagingUtils::toOrder).toList()));
		}
		int pageNumber = Optional.ofNullable(page.getPage()).orElse(0);
		org.springframework.data.domain.PageRequest p = org.springframework.data.domain.PageRequest.of(pageNumber, page.getPageSize());
		if (page.getSort() != null && !page.getSort().isEmpty())
			p = p.withSort(Sort.by(page.getSort().stream().map(PagingUtils::toOrder).toList()));
		return p;
	}
	
	public static Order toOrder(PageRequest.Sort sort) {
		if (PageRequest.SortOrder.DESC.equals(sort.getOrder()))
			return Order.desc(sort.getField());
		return Order.asc(sort.getField());
	}
	
}
