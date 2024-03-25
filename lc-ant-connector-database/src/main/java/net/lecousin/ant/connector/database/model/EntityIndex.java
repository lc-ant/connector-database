package net.lecousin.ant.connector.database.model;

import java.util.LinkedList;
import java.util.List;

import lombok.Data;

@Data
public class EntityIndex {

	private String name;
	private List<String> fields = new LinkedList<>();
	private IndexType type;
	
}
