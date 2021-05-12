package com.vpedak.talend.components.source;

import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.dataset.InputDataset;
import com.vpedak.talend.components.dataset.InputDataset.EntityEnum;
import com.vpedak.talend.components.processor.IRequestConfiguration;

public class InputGuessSchemaHelper {
	private final EntityEnum entity;
	private final IRequestConfiguration config;
	private final RecordBuilderFactory builderFactory;
	private SearchClient searchClient;
	private Record record;
	private String token;
	private List<String> langs;
	private List<Attribute> attrs;

	public InputGuessSchemaHelper(EntityEnum entity, RecordBuilderFactory builderFactory, SearchClient searchClient, IRequestConfiguration config) {
		this.entity = entity;
		this.builderFactory = builderFactory;
		this.searchClient = searchClient;
		this.config = config;
		
		init();
	}

	private void init() {
		token = Utils.getToken(config, searchClient);
		
		JsonObject json = Utils.getJson(searchClient.post("application/json", token, Utils.wrapRequest("query { getLanguages { identifier } }")));
		JsonArray arr = json.getJsonObject("data").getJsonArray("getLanguages");
		langs = arr.getValuesAs((JsonObject v) -> v.getString("identifier"));
		
		if (entity == EntityEnum.ITEM || entity == EntityEnum.ITEM_RELATION) {
			String attrQuery = 
					"query { search(\r\n" + 
					"    requests: [\r\n" + 
					"        {\r\n" + 
					"            entity: ATTRIBUTE, \r\n" + 
					"            offset: 0, \r\n" + 
					"            limit: 100,\r\n" + 
					"        }]\r\n" + 
					"    ) {\r\n" + 
					"    responses {\r\n" + 
					"        ... on AttributesResponse {\r\n" + 
					"            rows {\r\n" + 
					"                identifier\r\n" + 
					"                languageDependent\r\n" + 
					"                valid\r\n" + 
					"                relations\r\n" + 
					"            }\r\n" + 
					"        }\r\n" + 
					"    }}}";
			json = Utils.getJson(searchClient.post("application/json", token, Utils.wrapRequest(attrQuery)));
			arr = json.getJsonObject("data").getJsonObject("search").getJsonArray("responses").get(0).asJsonObject().getJsonArray("rows");
			attrs = arr.getValuesAs((JsonObject v) -> 
				new Attribute(v.getString("identifier"), 
						v.getBoolean("languageDependent"), 
						v.getJsonArray("valid").size() > 0,
						v.getJsonArray("relations").size() > 0));
		}
	}

	private void initRecord() {
		if (entity == EntityEnum.ITEM) {
			initItemRecord(langs);
		} else if (entity == EntityEnum.TYPE) {
			initTypeRecord(langs);
		} else if (entity == EntityEnum.ATTRIBUTE_GROUP) {
			initAttrGroupRecord(langs);
		} else if (entity == EntityEnum.ATTRIBUTE) {
			initAttrRecord(langs);
		} else if (entity == EntityEnum.RELATION) {
			initRelationRecord(langs);
		} else if (entity == EntityEnum.ITEM_RELATION) {
			initItemRelRecord(langs);
		} else if (entity == EntityEnum.USER) {
			initUserRecord();
		} else if (entity == EntityEnum.ROLE) {
			initRoleRecord();
		} else if (entity == EntityEnum.LOV) {
			initLOVRecord(langs);
		}
	}

	private void initLOVRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();

		initCommonFields(langs, builder);

		builder.withString("values", "");

		record = builder.build();
	}
	
	private void initRoleRecord() {
		Record.Builder builder = builderFactory.newRecordBuilder();

		builder.withString("id", "");
		builder.withString("identifier", "");
		builder.withString("createdBy", "");
		builder.withString("createdAt", "");
		builder.withString("updatedBy", "");
		builder.withString("updatedAt", "");
		
		builder.withString("name", "");
		builder.withString("configAccess", "");
		builder.withString("relAccess", "");
		builder.withString("itemAccess", "");
		
		record = builder.build();
	}

	private void initUserRecord() {
		Record.Builder builder = builderFactory.newRecordBuilder();

		builder.withString("id", "");
		builder.withString("login", "");
		builder.withString("createdBy", "");
		builder.withString("createdAt", "");
		builder.withString("updatedBy", "");
		builder.withString("updatedAt", "");
		
		builder.withString("name", "");
		builder.withString("email", "");
		builder.withString("props", "");
		builder.withString("roles", "");
		
		record = builder.build();
	}
	
	private void initItemRelRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();

		builder.withString("id", "");
		builder.withString("identifier", "");
		builder.withString("createdBy", "");
		builder.withString("createdAt", "");
		builder.withString("updatedBy", "");
		builder.withString("updatedAt", "");
		
		builder.withString("relationId", "");
		builder.withString("relationIdentifier", "");
		builder.withString("itemId", "");
		builder.withString("itemIdentifier", "");
		builder.withString("targetId", "");
		builder.withString("targetIdentifier", "");
		
		buildValues(langs, builder);
		
		record = builder.build();
	}
	
	private void initRelationRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();
		
		initCommonFields(langs, builder);
		builder.withBoolean("child", false);
		builder.withBoolean("multi", false);
		builder.withString("sources", "");
		builder.withString("targets", "");
				
		record = builder.build();
	}
	
	private void initAttrRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();
		
		initCommonFields(langs, builder);
		builder.withLong("order", 0);
		builder.withBoolean("languageDependent", false);
		builder.withString("valid", "");
		builder.withString("visible", "");
		builder.withString("relations", "");
		builder.withString("groups", "");
		builder.withLong("type", 0);
		builder.withString("pattern", "");
		builder.withString("errorMessage", "");
		builder.withString("lov", "");
		builder.withBoolean("richText", false);
		builder.withBoolean("multiLine", false);
		
		record = builder.build();
	}
	
	private void initAttrGroupRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();
		
		initCommonFields(langs, builder);
		builder.withString("order", "");
		builder.withBoolean("visible", false);
		
		record = builder.build();
	}
	
	private void initTypeRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();
		
		initCommonFields(langs, builder);
		builder.withString("path", "");
		builder.withString("parentIdentifier", "");
		builder.withString("link", "");
		builder.withString("linkIdentifier", "");
		builder.withString("icon", "");
		builder.withString("iconColor", "");
		builder.withBoolean("file", false);
		builder.withString("mainImage", "");
		builder.withString("images", "");
		
		record = builder.build();
	}
	
	private void initItemRecord(List<String> langs) {
		Record.Builder builder = builderFactory.newRecordBuilder();
		
		initCommonFields(langs, builder);
		builder.withString("parentIdentifier", "");
		builder.withString("path", "");
		builder.withString("typeId", "");
		builder.withString("typeIdentifier", "");
		builder.withString("mimeType", "");
		builder.withString("fileOrigName", "");
		
		buildValues(langs, builder);
		
		record = builder.build();
	}

	private void buildValues(List<String> langs, Record.Builder builder) {
		int idx = 1;
		for (Attribute attr : attrs) {
			if (attr.itemValues) {
				if (attr.languageDependent) {
					for(String lang : langs) builder.withString("attr_"+attr.identifier + "_" + lang, "");
				} else {
					builder.withString("attr_"+attr.identifier, "");
				}
				
				if (idx++ > 2) break;
			}
		}
	}

	private void initCommonFields(List<String> langs, Record.Builder builder) {
		builder.withString("id", "");
		builder.withString("identifier", "");
		for(String lang : langs) builder.withString("name_"+lang, "");
		builder.withString("createdBy", "");
		builder.withString("createdAt", "");
		builder.withString("updatedBy", "");
		builder.withString("updatedAt", "");
	}
	
	public Schema getSchema() { // not using now
		Schema.Builder builder = builderFactory.newSchemaBuilder(Type.RECORD);
		if (entity == EntityEnum.ITEM) {
			builder.withEntry(builderFactory.newEntryBuilder().withName("id").withType(Type.STRING).build());
			builder.withEntry(builderFactory.newEntryBuilder().withName("identifier").withType(Type.STRING).build());
		}
		return builder.build();
	}

	public Record next() {
		if (record == null) {
			initRecord();
			return record;
		} else {
			return null;
		}
	}
	
	private static class Attribute {
		public String identifier;
		public boolean languageDependent;
		public boolean itemValues;
		public boolean relationValues;
		public Attribute(String identifier, boolean languageDependent, boolean itemValues, boolean relationValues) {
			this.identifier = identifier;
			this.languageDependent = languageDependent;
			this.itemValues = itemValues;
			this.relationValues = relationValues;
		}
	}
}
