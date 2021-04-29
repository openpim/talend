package com.vpedak.talend.components.source;

import java.util.Iterator;
import java.util.function.Supplier;

import javax.json.JsonObject;
import javax.json.JsonValue;

import org.talend.sdk.component.api.base.BufferizedProducerSupport;
import org.talend.sdk.component.api.service.http.Response;

import com.vpedak.talend.components.dataset.InputDataset.EntityEnum;

public class MyBufferizedProducerSupport extends BufferizedProducerSupport<JsonValue > {
	public MyBufferizedProducerSupport(SearchClient searchClient, String token, PIMInputMapperConfiguration config) {
		super(new Supplier<Iterator<JsonValue>>() {
			private transient int offset = 0;
			private transient int count = -1;
			@Override
			public Iterator<JsonValue> get() {
				int limit = config.getDataset().getPageSize() == 0 ? 1000 : config.getDataset().getPageSize();
				if (count != -1) {
					if (count <= offset + limit) {
						return null;
					} else {
						offset = offset + limit;
					}
				}
					
				String query = generateQuery(config, offset, limit);
				if (config.getDataset().getDebugOutput()) {
					System.out.println("Sending request: " + query);
				}
				Response<JsonObject> response = searchClient.post("application/json", token, Utils.wrapRequest(query));
				JsonObject result = Utils.getJson(response);
				
				if (config.getDataset().getDebugOutput()) {
					System.out.println("Received response: " + result);
				}
				
				JsonObject searchResponse = result.getJsonObject("data").getJsonObject("search").getJsonArray("responses").get(0).asJsonObject();
				count = searchResponse.getInt("count");

				if (config.getDataset().getDebugOutput()) {
					System.out.println("Found count: " + count);
				}

				return searchResponse.getJsonArray("rows").iterator();
			}
		});
	}

	protected static String generateQuery(PIMInputMapperConfiguration config, int offset, int limit) {
		String query = "query { search(\n" + 
				"    requests: [\n" + 
				"        {\n" + 
				"            entity: "+config.getDataset().getEntity()+", \n" + 
				"            offset: "+offset+", \n" + 
				"            limit: "+limit+",\n" + 
				"            where: "+(config.getDataset().getWhere() != null &&config.getDataset().getWhere().length() > 0 ? config.getDataset().getWhere() : "{}" )+",\n" + 
				"            order: "+(config.getDataset().getOrder() != null &&config.getDataset().getOrder().length() > 0 ? config.getDataset().getOrder() : "[]" )+",\n" + 
				"        }]\n" + 
				"    ) {\n" + 
				"    responses {\n";
		
				if (config.getDataset().getEntity() == EntityEnum.ITEM) query += itemFields();
				else if (config.getDataset().getEntity() == EntityEnum.TYPE) query += typeFields();
				else if (config.getDataset().getEntity() == EntityEnum.ATTRIBUTE_GROUP) query += attrGroupFields();
				else if (config.getDataset().getEntity() == EntityEnum.ATTRIBUTE) query += attrFields();
				else if (config.getDataset().getEntity() == EntityEnum.RELATION) query += relationFields();
				else if (config.getDataset().getEntity() == EntityEnum.ITEM_RELATION) query += itemRelFields();
				else if (config.getDataset().getEntity() == EntityEnum.USER) query += userFields();
				else if (config.getDataset().getEntity() == EntityEnum.ROLE) query += roleFields();
				else if (config.getDataset().getEntity() == EntityEnum.LOV) query += lovFields();
				
				query += "    }}}";
		return query;
	}

	private static String lovFields() {
		return 	"        ... on LOVsResponse {\n" + 
				"            count\n" + 
				"            rows {\n" +
				"			  id\n" + 
				"			  identifier\n" + 
				"			  name\n" + 
				"			  values { id value }\n" + 
				"			  createdBy\n" + 
				"			  createdAt\n" + 
				"			  updatedBy\n" + 
				"			  updatedAt\n" + 
				"            }\n" + 
				"        }\n"; 
	}		
	
	private static String roleFields() {
		return 	"        ... on RolesResponse {\n" + 
				"            count\n" + 
				"            rows {\n" +
				"			  id\n" + 
				"			  identifier\n" + 
				"			  name\n" + 
				"			  configAccess\n" + 
				"			  relAccess\n" + 
				"			  itemAccess\n" + 
				"			  createdBy\n" + 
				"			  createdAt\n" + 
				"			  updatedBy\n" + 
				"			  updatedAt\n" + 
				"            }\n" + 
				"        }\n"; 
	}		
	private static String userFields() {
		return 	"        ... on UsersResponse {\n" + 
				"            count\n" + 
				"            rows {\n" +
				"			  id\n" + 
				"			  login\n" + 
				"			  name\n" + 
				"			  roles\n" + 
				"			  email\n" + 
				"			  props\n" + 
				"			  createdBy\n" + 
				"			  createdAt\n" + 
				"			  updatedBy\n" + 
				"			  updatedAt\n" + 
				"            }\n" + 
				"        }\n"; 
	}
	
	private static String itemFields() {
		return 	"        ... on ItemsSearchResponse {\n" + 
				"            count\n" + 
				"            rows {\n" +
				"			  id\n" + 
				"			  path\n" + 
				"			  identifier\n" + 
				"			  name\n" + 
				"			  typeId\n" + 
				"			  typeIdentifier\n" + 
				"			  parentIdentifier\n" + 
				"			  values\n" + 
				"			  mimeType\n" + 
				"			  fileOrigName\n" + 
				"			  createdBy\n" + 
				"			  createdAt\n" + 
				"			  updatedBy\n" + 
				"			  updatedAt\n" + 
				"            }\n" + 
				"        }\n"; 
	}
	
	private static String typeFields() {
		return "        ... on TypesResponse {\n" + 
				"            count\n" + 
				"            rows {\n" + 
				"				  id\n" + 
				"				  identifier\n" + 
				"				  name\n" + 
				"				  path\n" + 
				"				  parentIdentifier\n" + 
				"				  link\n" + 
				"				  linkIdentifier\n" + 
				"				  icon\n" + 
				"				  iconColor\n" + 
				"				  file\n" + 
				"				  mainImage\n" + 
				"				  images\n" + 
				"				  createdBy\n" + 
				"				  createdAt\n" + 
				"				  updatedBy\n" + 
				"				  updatedAt\n" + 
				"            }\n" + 
				"        }\n"; 
	}

	private static String attrGroupFields() {
		return "        ... on AttrGroupsResponse {\n" + 
				"            count\n" + 
				"            rows {\n" + 
				"				  id\n" + 
				"				  identifier\n" + 
				"				  name\n" + 
				"				  order\n" + 
				"				  visible\n" + 
				"				  createdBy\n" + 
				"				  createdAt\n" + 
				"				  updatedBy\n" + 
				"				  updatedAt\n" + 
				"            }\n" + 
				"        }\n" + 
				"";
	}
	
	
	private static String attrFields() {
		return "        ... on AttributesResponse {\n" + 
		"            count\n" + 
		"            rows {\n" + 
		"			  id\n" + 
		"			  identifier\n" + 
		"			  name\n" + 
		"			  order\n" + 
		"			  valid\n" + 
		"			  visible\n" + 
		"			  relations\n" + 
		"			  languageDependent\n" + 
		"			  groups\n" + 
		"			  type\n" + 
		"			  pattern\n" + 
		"			  errorMessage\n" + 
		"			  lov\n" + 
		"			  richText\n" + 
		"			  multiLine\n" + 
		"			  options\n" + 
		"			  createdBy\n" + 
		"			  createdAt\n" + 
		"			  updatedBy\n" + 
		"			  updatedAt\n" + 
		"            }\n" + 
		"        }";
	}
	
	private static String relationFields() {
		return "        ... on RelationsResponse {\n" + 
				"            count\n" + 
				"            rows {\n" + 
				"			  id\n" + 
				"			  identifier\n" + 
				"			  name\n" + 
				"			  child\n" + 
				"			  multi\n" + 
				"			  sources\n" + 
				"			  targets\n" + 
				"			  createdBy\n" + 
				"			  createdAt\n" + 
				"			  updatedBy\n" + 
				"			  updatedAt\n" + 
				"			}\n" + 
				"        }\n"; 
	}
	
	private static String itemRelFields() {
		return "        ... on SearchItemRelationResponse {\n" + 
				"            count\n" + 
				"            rows {\n" + 
				"			  id\n" + 
				"			  identifier\n" + 
				"			  relationId\n" + 
				"			  relationIdentifier\n" + 
				"			  itemId\n" + 
				"			  itemIdentifier\n" + 
				"			  targetId\n" + 
				"			  targetIdentifier\n" + 
				"			  values\n" + 
				"			  createdBy\n" + 
				"			  createdAt\n" + 
				"			  updatedBy\n" + 
				"			  updatedAt\n" + 
				"			}\n" + 
				"        }";
	}		
}