package net.lecousin.ant.connector.database.mongodb.mappers;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import net.lecousin.ant.core.mapping.ClassMapper;

@Service
public class ObjectIdToString implements ClassMapper<ObjectId, String> {

	@Override
	public String map(ObjectId source) {
		return source.toHexString();
	}

	@Override
	public Class<ObjectId> sourceType() {
		return ObjectId.class;
	}

	@Override
	public Class<String> targetType() {
		return String.class;
	}

}
