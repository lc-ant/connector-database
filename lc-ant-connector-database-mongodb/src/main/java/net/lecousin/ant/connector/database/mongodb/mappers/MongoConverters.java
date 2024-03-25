package net.lecousin.ant.connector.database.mongodb.mappers;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MongoConverters {

	public static Object toMongo(Object value) {
		if (value == null) return null;
		if (value instanceof Instant i)
			return i.toEpochMilli();
		if (value instanceof LocalDate d)
			return d.toString();
		if (value instanceof LocalTime t)
			return t.toString();
		if (value instanceof LocalDateTime dt)
			return dt.toString();
		if (value instanceof Optional o)
			return o.isPresent() ? toMongo(o.get()) : null;
		return value;
	}
	
}
