package com.vpedak.talend.components.service;

import javax.json.JsonObject;
import com.vpedak.talend.components.source.Utils;


import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;

import com.vpedak.talend.components.datastore.CustomDatastore;
import com.vpedak.talend.components.source.SearchClient;

@Service
public class PimComponentsService {

	@HealthCheck
    public HealthCheckStatus testConnection(final CustomDatastore datastore, final SearchClient searchClient) {
    	String request = "mutation {signIn(login: \"" + datastore.getUsername() + "\", password: \"" + datastore.getPassword() + "\") {\n" + 
    			"	  token\n" + 
    			"}}";
    	searchClient.base(Utils.prepareUrl(datastore.getUrl().toString()));
    	Response<JsonObject> response = searchClient.auth("application/json", request);
		if (response.status() == 200) {
			return new HealthCheckStatus(HealthCheckStatus.Status.OK, "OK");
		} else {
			return new HealthCheckStatus(HealthCheckStatus.Status.KO, response.error(String.class));
		}
    }
	
	/*
	@DiscoverSchema(value = "InputDataset")
	public Schema guessInputSchema(final InputDataset dataset, RecordBuilderFactory builderFactory, SearchClient searchClient) {
		return new GuessSchemaHelper(dataset, builderFactory, searchClient).getSchema();
	}*/	
}