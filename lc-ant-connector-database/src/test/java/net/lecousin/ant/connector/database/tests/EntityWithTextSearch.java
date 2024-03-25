package net.lecousin.ant.connector.database.tests;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.lecousin.ant.connector.database.annotations.Entity;
import net.lecousin.ant.connector.database.annotations.GeneratedValue;
import net.lecousin.ant.connector.database.annotations.Index;
import net.lecousin.ant.connector.database.model.IndexType;

@Entity(domain = "test", name = "text_search")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Index(fields = { "title", "content" }, type = IndexType.TEXT)
public class EntityWithTextSearch {

	@Id @GeneratedValue
	private String id;
	
	private String title;
	private String content;
	private String nonIndexed;
	
}
