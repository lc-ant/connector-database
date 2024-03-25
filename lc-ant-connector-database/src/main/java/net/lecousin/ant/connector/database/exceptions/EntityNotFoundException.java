package net.lecousin.ant.connector.database.exceptions;

import net.lecousin.ant.core.api.exceptions.ConflictException;
import net.lecousin.commons.io.text.i18n.TranslatedString;

public class EntityNotFoundException extends ConflictException {

	private static final long serialVersionUID = 1L;

	public EntityNotFoundException() {
		super(new TranslatedString("connector-database", "entity not found"), "database-entity-not-found");
	}
	
}
