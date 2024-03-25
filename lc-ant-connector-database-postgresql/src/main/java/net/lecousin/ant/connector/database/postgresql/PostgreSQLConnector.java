package net.lecousin.ant.connector.database.postgresql;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Assignment;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.r2dbc.core.DatabaseClient;

import net.lecousin.ant.connector.database.postgresql.sql.JsonbArrayAppendElement;
import net.lecousin.ant.connector.database.postgresql.sql.JsonbArrayRemoveElement;
import net.lecousin.ant.connector.database.r2dbc.ExtendedR2dbcDialect;
import net.lecousin.ant.connector.database.r2dbc.R2dbcConnector;
import net.lecousin.ant.connector.database.r2dbc.SqlQuery;
import net.lecousin.ant.core.patch.Patch;
import net.lecousin.ant.core.patch.PatchAppendElement;
import net.lecousin.ant.core.patch.PatchRemoveElement;

public class PostgreSQLConnector extends R2dbcConnector implements InitializingBean {

	public PostgreSQLConnector(DatabaseClient r2dbc) {
		super(r2dbc);
	}

	@Override
	protected ExtendedR2dbcDialect getExtendedDialect() {
		return new PostgresqlDialect(dialect.getIdentifierProcessing());
	}
	
	@Override
	protected Assignment patchToAssignment(Patch patch, Table table, SqlQuery<?> query) {
		if (patch instanceof PatchAppendElement p)
			return AssignValue.create(
				Column.create(SqlIdentifier.quoted(p.getFieldName()), table),
				new JsonbArrayAppendElement(Column.create(SqlIdentifier.quoted(p.getFieldName()), table), query.marker(p.getElementToAppend()), renderContext)
			);
		if (patch instanceof PatchRemoveElement p)
			return AssignValue.create(
				Column.create(SqlIdentifier.quoted(p.getFieldName()), table),
				new JsonbArrayRemoveElement(Column.create(SqlIdentifier.quoted(p.getFieldName()), table), query.marker(p.getElementToRemove()), renderContext)
			);
		return super.patchToAssignment(patch, table, query);
	}
	
}
