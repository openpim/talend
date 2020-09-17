package com.vpedak.talend.components.source;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

public interface SearchClient extends HttpClient { 

    @Request(path = "/graphql", method = "POST") 
    Response<JsonObject> auth( 
            @Header("Content-Type") String contentType,
            String query);
	
    @Request(path = "/graphql", method = "POST") 
    Response<JsonObject> post( 
            @Header("Content-Type") String contentType,
            @Header("x-token") String token,
            String body);

    @Request(path = "/asset-upload", method = "POST") 
    Response<String> upload( 
            @Header("Content-Type") String contentType,
            @Header("x-token") String token,
            byte[] body);
    
    @Request(path = "/asset/{id}", method = "GET") 
    Response<byte[]> download(
            @Header("x-token") String token,
    		@Path("id") String id);

}
