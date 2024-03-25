package net.lecousin.ant.connector.database.exceptions;

import net.lecousin.ant.core.api.exceptions.ConflictException;
import net.lecousin.commons.io.text.i18n.CompositeI18nString;
import net.lecousin.commons.io.text.i18n.TranslatedString;

public class DuplicatedKeyException extends ConflictException {

	private static final long serialVersionUID = 1L;

	public DuplicatedKeyException(String message) {
		super(CompositeI18nString.of(new TranslatedString("connector-database", "duplicated key"), ": " + message), "duplicated-key");
	}
	
}
