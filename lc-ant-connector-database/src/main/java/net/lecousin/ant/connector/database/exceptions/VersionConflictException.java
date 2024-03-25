package net.lecousin.ant.connector.database.exceptions;

import net.lecousin.ant.core.api.exceptions.ConflictException;
import net.lecousin.commons.io.text.i18n.TranslatedString;

public class VersionConflictException extends ConflictException {

	private static final long serialVersionUID = 1L;

	public VersionConflictException() {
		super(new TranslatedString("connector-database", "version conflict"), "version-conflict");
	}
	
}
