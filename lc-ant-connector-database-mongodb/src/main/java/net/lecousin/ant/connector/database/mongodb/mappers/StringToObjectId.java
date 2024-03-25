package net.lecousin.ant.connector.database.mongodb.mappers;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import net.lecousin.ant.core.mapping.ClassMapper;

@Service
public class StringToObjectId implements ClassMapper<String, ObjectId> {

	@Override
	public ObjectId map(String source) {
		return new ObjectId(source);
	}

	@Override
	public Class<String> sourceType() {
		return String.class;
	}

	@Override
	public Class<ObjectId> targetType() {
		return ObjectId.class;
	}

}
