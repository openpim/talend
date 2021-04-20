package com.vpedak.talend.components.processor;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.dataset.InputDataset.EntityEnum;
import com.vpedak.talend.components.service.PimComponentsService;
import com.vpedak.talend.components.source.OutputGuessSchemaHelper;
import com.vpedak.talend.components.source.SearchClient;
import com.vpedak.talend.components.source.Utils;

@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = CUSTOM, custom = "PIMOutput") // icon is located at src/main/resources/icons/PIMOutput.svg
@Processor(name = "Output")
@Documentation("TODO fill the documentation for this processor")
public class OutputProcessor implements Serializable {
    private final OutputProcessorConfiguration configuration;
    private final RecordBuilderFactory builderFactory;
    private final PimComponentsService service;
    private final SearchClient searchClient;
    private OutputEmitter<Record> defaultOutput;
    private OutputEmitter<Record> REJECTOutput;
	private boolean isGuessSchema = false;
	private OutputGuessSchemaHelper schemaHelper;
	private String token;
	private StringBuilder request;
	private StringBuilder list;
	private List<String> langs;

    public OutputProcessor(@Option("configuration") final OutputProcessorConfiguration configuration,
    		final RecordBuilderFactory builderFactory,
            final PimComponentsService service,
            final SearchClient searchClient) {
        this.configuration = configuration;
        this.builderFactory = builderFactory;
        this.searchClient = searchClient;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        String genisisClass = stackTraceElements[stackTraceElements.length -1].getClassName();
        if (genisisClass.contains("guess_schema"))
        {

            isGuessSchema = true;
        }
        
        if (isGuessSchema) {
        	schemaHelper = new OutputGuessSchemaHelper(builderFactory);
        } else {
        	searchClient.base(configuration.getDatastore().getUrl().toString());
			token = Utils.getToken(configuration, searchClient);
			
			JsonObject json = Utils.getJson(searchClient.post("application/json", token, Utils.wrapRequest("query { getLanguages { identifier } }")));
			JsonArray arr = json.getJsonObject("data").getJsonArray("getLanguages");
			langs = arr.getValuesAs((JsonObject v) -> v.getString("identifier"));		
        }
    }
    
    private String getEntity(OutputProcessorConfiguration configuration) {
    	if (configuration.getEntity() == EntityEnum.ITEM) {
    		return "items";
    	} else if (configuration.getEntity() == EntityEnum.ITEM_RELATION) {
    		return "itemRelations";
    	} else if (configuration.getEntity() == EntityEnum.TYPE) {
    		return "types";
    	} else if (configuration.getEntity() == EntityEnum.RELATION) {
    		return "relations";
    	} else if (configuration.getEntity() == EntityEnum.ATTRIBUTE_GROUP) {
    		return "attrGroups";
    	} else if (configuration.getEntity() == EntityEnum.ATTRIBUTE) {
    		return "attributes";
    	} else if (configuration.getEntity() == EntityEnum.ROLE) {
    		return "roles";
    	} else if (configuration.getEntity() == EntityEnum.USER) {
    		return "users";
    	} else if (configuration.getEntity() == EntityEnum.LOV) {
    		return "lovs";
    	} else {
    		return "???";
    	}
    }

    @BeforeGroup
    public void beforeGroup() {
    	list = new StringBuilder();
		request = new StringBuilder("mutation { import(\n" + 
				"    config: {\n" + 
				"        mode: " + configuration.getImportMode() + "\n" + 
				"        errors: " + configuration.getErrorProcessing() + "\n" + 
				"    },\n" + 
				"    " + getEntity(configuration) +": [\n" + 
				"");
    }

    @ElementListener
    public void onNext(
            @Input final Record defaultInput,
            @Output final OutputEmitter<Record> defaultOutput,
            @Output("REJECT") final OutputEmitter<Record> REJECTOutput) {
    	if (isGuessSchema) {
    		Record rec = schemaHelper.next();
    		defaultOutput.emit(rec);
    		REJECTOutput.emit(rec);
    	} else {
    		this.defaultOutput = defaultOutput;
    		this.REJECTOutput = REJECTOutput;
        	if (configuration.getEntity() == EntityEnum.ITEM) {
        		processItemRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.ITEM_RELATION) {
        		processItemRelationRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.TYPE) {
        		processTypeRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.RELATION) {
        		processRelationRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.ATTRIBUTE_GROUP) {
        		processAttrGroupRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.ATTRIBUTE) {
        		processAttributeRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.ROLE) {
        		processRoleRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.USER) {
        		processUserRecord(defaultInput);
        	} else if (configuration.getEntity() == EntityEnum.LOV) {
        		processLOVRecord(defaultInput);
        	}
    	}
    }

    private void processLOVRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("values")) appendValue(list, "values", entry, record);
			if (name.startsWith("name_")) {
				String lang = name.substring(5);
				appendValue(nameValues, lang, entry, record);
			}
		}
		
		if (nameValues.length() > 1) {
			nameValues.append("}");
			list.append("name: ").append(nameValues.toString()).append(",");
		}
	
    	list.append("},");
	}    
    private void processItemRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
    	StringBuilder values = new StringBuilder("{");
    	Map<String, StringBuilder> langAttributes = new HashMap<String, StringBuilder>();
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("typeIdentifier")) appendValue(list, "typeIdentifier", entry, record);
			if (name.equals("parentIdentifier")) appendValue(list, "parentIdentifier", entry, record);
			if (name.startsWith("name_")) {
				String lang = name.substring(5);
				appendValue(nameValues, lang, entry, record);
			}
			if (name.startsWith("attr_")) {
				String attrName = name.substring(5);
				int pos = attrName.lastIndexOf('_');
				if (pos == -1) {
					appendValue(values, attrName, entry, record);
				} else {
					String lang = attrName.substring(pos+1);
					if (langs.contains(lang)) {
						attrName = attrName.substring(0, pos);
						StringBuilder sb = langAttributes.get(attrName);
						if (sb == null) {
							sb = new StringBuilder("{");
							langAttributes.put(attrName, sb);
						}
						appendValue(sb, lang, entry, record);
					} else {
						// not a language dependent attribute
						appendValue(values, attrName, entry, record);
					}
				}
			}
		}
		
		if (nameValues.length() > 1) {
			nameValues.append("}");
			list.append("name: ").append(nameValues.toString()).append(",");
		}

		if (values.length() > 1 || langAttributes.size() > 0) {
			for (Map.Entry<String, StringBuilder> mapEntry: langAttributes.entrySet()) {
				mapEntry.getValue().append("}");
				values.append(mapEntry.getKey()).append(": ").append(mapEntry.getValue()).append(",");
			}
			values.append("}");
			list.append("values: ").append(values.toString());
		}
		
    	list.append("},");
	}
    
    private void processItemRelationRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder values = new StringBuilder("{");
    	Map<String, StringBuilder> langAttributes = new HashMap<String, StringBuilder>();
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("relationIdentifier")) appendValue(list, "relationIdentifier", entry, record);
			if (name.equals("itemIdentifier")) appendValue(list, "itemIdentifier", entry, record);
			if (name.equals("targetIdentifier")) appendValue(list, "targetIdentifier", entry, record);
			if (name.startsWith("attr_")) {
				String attrName = name.substring(5);
				int pos = attrName.lastIndexOf('_');
				if (pos == -1) {
					appendValue(values, attrName, entry, record);
				} else {
					String lang = attrName.substring(pos+1);
					if (langs.contains(lang)) {
						attrName = attrName.substring(0, pos);
						StringBuilder sb = langAttributes.get(attrName);
						if (sb == null) {
							sb = new StringBuilder("{");
							langAttributes.put(attrName, sb);
						}
						appendValue(sb, lang, entry, record);
					} else {
						// not a language dependent attribute
						appendValue(values, attrName, entry, record);
					}
				}
			}
		}
		
		if (values.length() > 1 || langAttributes.size() > 0) {
			for (Map.Entry<String, StringBuilder> mapEntry: langAttributes.entrySet()) {
				mapEntry.getValue().append("}");
				values.append(mapEntry.getKey()).append(": ").append(mapEntry.getValue()).append(",");
			}
			values.append("}");
			list.append("values: ").append(values.toString());
		}
		
    	list.append("},");
	}    
    
    
    private void processTypeRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("parentIdentifier")) appendValue(list, "parentIdentifier", entry, record);
			if (name.equals("linkIdentifier")) appendValue(list, "linkIdentifier", entry, record);
			if (name.equals("icon")) appendValue(list, "icon", entry, record);
			if (name.equals("iconColor")) appendValue(list, "iconColor", entry, record);
			if (name.equals("file")) appendValue(list, "file", entry, record);
			if (name.equals("mainImage")) appendValue(list, "mainImage", entry, record);
			if (name.equals("images")) appendValue(list, "images", entry, record);
			if (name.startsWith("name_")) {
				String lang = name.substring(5);
				appendValue(nameValues, lang, entry, record);
			}
		}
		
		if (nameValues.length() > 1) {
			nameValues.append("}");
			list.append("name: ").append(nameValues.toString()).append(",");
		}

    	list.append("},");
	}

    private void processRelationRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("sources")) appendValue(list, "sources", entry, record);
			if (name.equals("targets")) appendValue(list, "targets", entry, record);
			if (name.equals("child")) appendValue(list, "child", entry, record);
			if (name.equals("multi")) appendValue(list, "multi", entry, record);
			if (name.startsWith("name_")) {
				String lang = name.substring(5);
				appendValue(nameValues, lang, entry, record);
			}
		}
		
		if (nameValues.length() > 1) {
			nameValues.append("}");
			list.append("name: ").append(nameValues.toString()).append(",");
		}

    	list.append("},");
	}

    private void processAttrGroupRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("order")) appendValue(list, "order", entry, record);
			if (name.equals("visible")) appendValue(list, "visible", entry, record);
			if (name.startsWith("name_")) {
				String lang = name.substring(5);
				appendValue(nameValues, lang, entry, record);
			}
		}
		
		if (nameValues.length() > 1) {
			nameValues.append("}");
			list.append("name: ").append(nameValues.toString()).append(",");
		}

    	list.append("},");
	}
    
    private void processAttributeRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("valid")) appendValue(list, "valid", entry, record);
			if (name.equals("visible")) appendValue(list, "visible", entry, record);
			if (name.equals("relations")) appendValue(list, "relations", entry, record);
			if (name.equals("groups")) appendValue(list, "groups", entry, record);
			if (name.equals("order")) appendValue(list, "order", entry, record);
			if (name.equals("languageDependent")) appendValue(list, "languageDependent", entry, record);
			if (name.equals("type")) appendValue(list, "type", entry, record);
			if (name.equals("pattern")) appendValue(list, "pattern", entry, record);
			if (name.equals("errorMessage")) appendValue(list, "errorMessage", entry, record);
			if (name.equals("lov")) appendValue(list, "lov", entry, record);
			if (name.equals("richText")) appendValue(list, "richText", entry, record);
			if (name.equals("multiLine")) appendValue(list, "multiLine", entry, record);
			if (name.equals("options")) appendValue(list, "options", entry, record);
			if (name.startsWith("name_")) {
				String lang = name.substring(5);
				appendValue(nameValues, lang, entry, record);
			}
		}
		
		if (nameValues.length() > 1) {
			nameValues.append("}");
			list.append("name: ").append(nameValues.toString()).append(",");
		}

    	list.append("},");
	}
    
    private void processRoleRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("identifier")) appendValue(list, "identifier", entry, record);
			if (name.equals("name")) appendValue(list, "name", entry, record);
			if (name.equals("configAccess")) appendValue(list, "configAccess", entry, record);
			if (name.equals("relAccess")) appendValue(list, "relAccess", entry, record);
			if (name.equals("itemAccess")) appendValue(list, "itemAccess", entry, record);
		}
    	list.append("},");
	}
    
    private void processUserRecord(Record record) {
    	list.append("{");
    	Schema schema = record.getSchema();
    	StringBuilder nameValues = new StringBuilder("{");
		for (Entry entry : schema.getEntries()) {
			String name = entry.getName();
			if (name.equals("delete")) appendValue(list, "delete", entry, record);
			if (name.equals("login")) appendValue(list, "login", entry, record);
			if (name.equals("name")) appendValue(list, "name", entry, record);
			if (name.equals("roles")) appendValue(list, "roles", entry, record);
			if (name.equals("email")) appendValue(list, "email", entry, record);
			if (name.equals("props")) appendValue(list, "props", entry, record);
		}
    	list.append("},");
	}
    
    private void appendValue(StringBuilder sb, String name, Entry entry, Record record) {
    	Object tmp = record.get(Object.class, entry.getName());
    	if (tmp == null) return;
    		
    	sb.append(name).append(": ");
    	boolean encode = false;
    	if (entry.getType() == Type.STRING && 
    			!name.equals("sources") && 
    			!name.equals("targets") && 
    			!name.equals("valid") && 
    			!name.equals("visible") && 
    			!name.equals("groups") && 
    			!name.equals("configAccess") && 
    			!name.equals("relAccess") && 
    			!name.equals("itemAccess") && 
    			!name.equals("roles") && 
    			!name.equals("props") && 
    			!name.equals("values") && 
    			!name.equals("images") && 
    			!name.equals("errorMessage") && 
    			!name.equals("options") && 
    			!name.equals("relations")) {
    		sb.append("\"\"\"");
    		encode = true;
    	}
    	Object obj = "";
    	if (entry.getType() == Type.BOOLEAN) {
    		Optional<Boolean> tst = record.getOptionalBoolean(entry.getName());
    		if (tst.isPresent()) obj = tst.get();
    			else obj = "null";
    	} else if (entry.getType() == Type.INT) {
    		OptionalInt tst = record.getOptionalInt(entry.getName());
    		if (tst.isPresent()) obj = tst.getAsInt();
    			else obj = "null";
    	} else if (entry.getType() == Type.LONG) {
    		OptionalLong tst = record.getOptionalLong(entry.getName());
    		if (tst.isPresent()) obj = tst.getAsLong();
    			else obj = "null";
    	} else if (entry.getType() == Type.DOUBLE) {
    		OptionalDouble tst = record.getOptionalDouble(entry.getName());
    		if (tst.isPresent()) obj = tst.getAsDouble();
    			else obj = "null";
    	} else if (entry.getType() == Type.FLOAT) {
    		OptionalDouble tst = record.getOptionalFloat(entry.getName());
    		if (tst.isPresent()) obj = tst.getAsDouble();
    			else obj = "null";
    	} else {
    		Optional<String> tst = record.getOptionalString(entry.getName());
    		if (tst.isPresent()) {
	    		String str = tst.get();
	    		if (encode)	obj = str.replace("\"", "\" "); //str.replace("\\", "\\\\").replace("\"", "\\\" ");
	    			else obj = str;
    		}
    	}
    	sb.append(obj);
    	if (entry.getType() == Type.STRING && 
    			!name.equals("sources") && 
    			!name.equals("targets") && 
    			!name.equals("valid") && 
    			!name.equals("visible") && 
    			!name.equals("groups") && 
    			!name.equals("configAccess") && 
    			!name.equals("relAccess") && 
    			!name.equals("itemAccess") && 
    			!name.equals("roles") && 
    			!name.equals("props") && 
    			!name.equals("values") && 
    			!name.equals("images") && 
    			!name.equals("errorMessage") && 
    			!name.equals("options") && 
    			!name.equals("relations")) {
    		sb.append("\"\"\",\n");
    	} else {
    		sb.append(",\n");
    	}
    }
    
	@AfterGroup
    public void afterGroup() {
		if (isGuessSchema) return;
			
		request.append(list.toString());
		request.append("    ]\n" + 
				"    ) {\n" + 
				"    " + getEntity(configuration) +" {\n" + 
				"	  identifier\n" + 
				"	  result\n" + 
				"	  id\n" + 
				"	  errors { code message }\n" + 
				"	  warnings { code message }\n" + 
				"	}}}");
		String req = request.toString();
		
		if (configuration.getDebugOutput()) {
			System.out.println("Sending import request: " + req);
		}
		
		Response<JsonObject> response = searchClient.post("application/json", token, Utils.wrapRequest(req));
		JsonObject result = Utils.getJson(response);
		
		if (configuration.getDebugOutput()) {
			System.out.println("Received response: " + result);
		}
		
		JsonArray arr = result.getJsonObject("data").getJsonObject("import").getJsonArray(getEntity(configuration));
		for (JsonValue data : arr) {
			JsonObject obj = data.asJsonObject();
			
			Record.Builder builder = builderFactory.newRecordBuilder();
			builder.withString("id", obj.getString("id"));
			builder.withString("identifier", obj.getString("identifier"));
			builder.withString("result", obj.getString("result"));
			
			JsonArray warns = obj.getJsonArray("warnings");
			for (int i=0; i< warns.size(); i++) builder.withString("warning"+(i+1), warns.get(i).asJsonObject().getString("message"));

			JsonArray errors = obj.getJsonArray("errors");
			for (int i=0; i< errors.size(); i++) builder.withString("error"+(i+1), errors.get(i).asJsonObject().getString("message"));
			
			Record record = builder.build();
			
			if (obj.getString("result").equals("REJECTED")) {
				REJECTOutput.emit(record);
			} else {
				defaultOutput.emit(record);
			}
		}
    }
}