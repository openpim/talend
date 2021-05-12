package com.vpedak.talend.components.source;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Response;

import com.vpedak.talend.components.dataset.InputDataset;
import com.vpedak.talend.components.processor.IRequestConfiguration;

public class Utils {

	public static String getToken(IRequestConfiguration config, SearchClient searchClient) {
		String request = "mutation {signIn(login: \"" + config.getDatastore().getUsername()
				+ "\", password: \"" + config.getDatastore().getPassword() + "\") {\n"
				+ "	  token\n" + "}}";
		if (config.getDebugOutput()) {
			System.out.println("Sending auth request: " + request);
		}
		Response<JsonObject> response = searchClient.auth("application/json", wrapRequest(request));
		JsonObject json = getJson(response);
		if (config.getDebugOutput()) {
			System.out.println("Received response: " + json);
		}
		
		String token = json.getJsonObject("data").getJsonObject("signIn").getString("token");

		if (config.getDebugOutput()) {
			System.out.println("Found token: " + token);
		}
		
		return token;
	}
	
	public static String wrapRequest(String request) {
		return "{\"query\":\""+request.replace("\"","\\\"").replace("\r", "").replace("\n", "").replace("\t", " ")+"\"}";
	}
	
	public static JsonObject getJson(Response<JsonObject> response) {
		if (response.status() == 200) {
			return response.body();
		}

		throw new RuntimeException(response.error(String.class));
	}
	
	public static String prepareUrl(String url) {
		if (url.endsWith("/")) {
			return url.substring(0, url.length()-1);
		} else {
			return url;
		}
	}
}
