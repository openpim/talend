package com.vpedak.talend.components.source;

import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import javax.xml.ws.handler.MessageContext.Scope;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.dataset.InputDataset;
import com.vpedak.talend.components.dataset.InputDataset.EntityEnum;

@Documentation("TODO fill the documentation for this source")
public class PIMInputSource implements Serializable {
	Logger logger = Logger.getLogger("PIMInputSource");
	private final PIMInputMapperConfiguration configuration;
	private final RecordBuilderFactory builderFactory;
	private SearchClient searchClient;
	private MyBufferizedProducerSupport producerSupport;
	private InputGuessSchemaHelper shemaHelper;
	private boolean isGuessSchema = false;
	private Record initRecord = null;

	public PIMInputSource(@Option("configuration") final PIMInputMapperConfiguration configuration,
			final RecordBuilderFactory builderFactory,
			final SearchClient searchClient) {
		this.configuration = configuration;
		this.builderFactory = builderFactory;
		this.searchClient = searchClient;
	}

	@PostConstruct
	public void init() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        String genisisClass = stackTraceElements[stackTraceElements.length -1].getClassName();
        if (genisisClass.contains("guess_schema"))
        {

            isGuessSchema = true;
        }
		
		if (!isGuessSchema) {
			String token = Utils.getToken(configuration.getDataset(), searchClient); 
					
			producerSupport = new MyBufferizedProducerSupport(searchClient, token, configuration);
		} else {
			shemaHelper = new InputGuessSchemaHelper(configuration.getDataset().getEntity(), builderFactory, searchClient, configuration.getDataset());
		}
	}

	@Producer
	public Record next() {
		if (!isGuessSchema) {
	        final JsonValue next = producerSupport.next();
	        
			if (configuration.getDataset().getDebugOutput()) {
				System.out.println("Next row: " + next);
			}
	        
	        return next == null ? null : buildRecord(next.asJsonObject(), configuration.getDataset());
		} else {
			return shemaHelper.next();
		}
    }

	public void setInitRecord(Record initRecord) {
		this.initRecord = initRecord;
	}

	private Record buildRecord(JsonObject json, InputDataset dataset) {
		Record.Builder builder = builderFactory.newRecordBuilder();
		if (initRecord != null) {
			copyRecord(initRecord, builder);
		}

		for (Entry<String, JsonValue> entry : json.entrySet()) {
			if (entry.getKey().equals("name")) {
				if (configuration.getDataset().getEntity() == EntityEnum.ROLE || configuration.getDataset().getEntity() == EntityEnum.USER) {
					buildValue(builder, entry.getKey(), entry.getValue());
				} else {
					for (Entry<String, JsonValue> name : entry.getValue().asJsonObject().entrySet())  {
						buildValue(builder, "name_"+name.getKey(), name.getValue());
					}
				}
			} else if (entry.getKey().equals("values") && (entry.getValue().getValueType() == ValueType.OBJECT || entry.getValue().getValueType() == ValueType.ARRAY)) {
				if (configuration.getDataset().getEntity() == EntityEnum.LOV) {
					builder.withString(entry.getKey(), dumpArray(entry.getValue().asJsonArray()));
				} else {
					
					for (Entry<String, JsonValue> values : entry.getValue().asJsonObject().entrySet())  {
						if (values.getValue().getValueType() != ValueType.OBJECT) {
							buildValue(builder, "attr_"+values.getKey(), values.getValue());
						} else {
							for (Entry<String, JsonValue> lang : values.getValue().asJsonObject().entrySet())  {
								buildValue(builder, "attr_"+values.getKey()+"_"+lang.getKey(), lang.getValue());
							}
						}
					}
					
				}
			} else if (!entry.getKey().equals("values")) {
				buildValue(builder, entry.getKey(), entry.getValue());
			}
		}
		
		return builder.build();
	}
	
	private void copyRecord(Record source, Builder builder) {
		Schema schema = source.getSchema();
		for (Schema.Entry entry : schema.getEntries()) {
			if (entry.getType() == Type.STRING) {
				source.getOptionalString(entry.getName()).ifPresent(v -> builder.withString(entry.getName(), v));
			} else if (entry.getType() == Type.INT) {
				source.getOptionalInt(entry.getName()).ifPresent(v -> builder.withInt(entry.getName(), v));
			} else if (entry.getType() == Type.LONG) {
				source.getOptionalLong(entry.getName()).ifPresent(v -> builder.withLong(entry.getName(), v));
			} else if (entry.getType() == Type.FLOAT) {
				source.getOptionalFloat(entry.getName()).ifPresent(v -> builder.withDouble(entry.getName(), v));
			} else if (entry.getType() == Type.DOUBLE) {
				source.getOptionalDouble(entry.getName()).ifPresent(v -> builder.withDouble(entry.getName(), v));
			} else if (entry.getType() == Type.BOOLEAN) {
				source.getOptionalBoolean(entry.getName()).ifPresent(v -> builder.withBoolean(entry.getName(), v));
			}
		}
	}

	private void buildValue(Record.Builder builder, String key, JsonValue value) {
		if (value.getValueType() == ValueType.STRING) {
			builder.withString(key, ((JsonString)value).getString());
		} else if (value.getValueType() == ValueType.TRUE) {
			// builder.withBoolean(key, true);
			builder.withString(key, "true");
		} else if (value.getValueType() == ValueType.FALSE) {
			// builder.withBoolean(key, false);
			builder.withString(key, "false");
		} else if (value.getValueType() == ValueType.OBJECT) {
			builder.withString(key, dumpObject(value.asJsonObject()));
		} else if (value.getValueType() == ValueType.NUMBER) {
			String val = value.toString();
			if (val.indexOf('.') != -1) {
				builder.withDouble(key, Double.valueOf(val));
			} else {
				builder.withLong(key, Long.valueOf(val));
			}
		} else {
			builder.withString(key, value.toString());
		}
	}
	
	private String dumpObject(JsonObject obj) {
		StringBuilder sb = new StringBuilder("{");
		
		for (Entry<String, JsonValue> entry : obj.entrySet()) {
			sb.append(entry.getKey()).append(": ");
			if (entry.getValue().getValueType() == ValueType.OBJECT) {
				sb.append(dumpObject(entry.getValue().asJsonObject()));
			} else if (entry.getValue().getValueType() == ValueType.ARRAY) {
				JsonArray arr = entry.getValue().asJsonArray();
				sb.append(dumpArray(arr));
			} else {
				sb.append(entry.getValue().toString());
			}
			sb.append(",");
		}
		if (obj.entrySet().size() > 0) sb.deleteCharAt(sb.length()-1);
		sb.append("}");
		
		return sb.toString();
	}

	private String dumpArray(JsonArray arr) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for( JsonValue val : arr) {
			if (val.getValueType() == ValueType.OBJECT) {
				sb.append(dumpObject(val.asJsonObject()));
			} else {
				sb.append(val.toString());
			}
			sb.append(",");
		}
		if (arr.size() > 0) sb.deleteCharAt(sb.length()-1);
		sb.append("]");
		
		return sb.toString();
	}
}