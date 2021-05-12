package com.vpedak.talend.components.processor;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import javax.annotation.PostConstruct;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
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
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.source.OutputGuessSchemaHelper;
import com.vpedak.talend.components.source.SearchClient;
import com.vpedak.talend.components.source.Utils;

@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = CUSTOM, custom = "PIMAssetDownload") // icon is located at src/main/resources/icons/PIMAssetDownload.svg
@Processor(name = "AssetDownload")
@Documentation("TODO fill the documentation for this processor")
public class DownloadProcessor implements Serializable {
	private final AssetsProcessorConfiguration configuration;
    private final RecordBuilderFactory builderFactory;
    private final SearchClient searchClient;
	private boolean isGuessSchema = false;
	private OutputGuessSchemaHelper schemaHelper;
	private String token;

    public DownloadProcessor(@Option("configuration") final AssetsProcessorConfiguration configuration,
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
        
        if (isGuessSchema) {
        	schemaHelper = new OutputGuessSchemaHelper(builderFactory, true);
        } else {
        	searchClient.base(Utils.prepareUrl(configuration.getDatastore().getUrl().toString()));
			token = Utils.getToken(configuration, searchClient);
        }
    }
    
    @BeforeGroup
    public void beforeGroup() {
    }

    @ElementListener
    public void onNext(
            @Input final Record in,
            @Output final OutputEmitter<Record> defaultOutput,
            @Output("REJECT") final OutputEmitter<Record> REJECTOutput) {
    	if (isGuessSchema) {
    		Record rec = schemaHelper.next();
    		defaultOutput.emit(rec);
    		REJECTOutput.emit(rec);
    	} else {
    		Record.Builder builder = builderFactory.newRecordBuilder();
			builder.withString("result", "ERROR");

    		String id = in.getString("id");
    		if (id == null || id.length() == 0) {
    			builder.withString("error", "Failed to find 'id' parameter in data record");
    			REJECTOutput.emit(builder.build());
    			return;
    		} else {
    			builder.withString("id", id);
    		}
    		String file = in.getString("file");
    		if (file == null || file.length() == 0) {
    			builder.withString("error", "Failed to find 'file' parameter in data record");
    			REJECTOutput.emit(builder.build());
    			return;
    		} else {
    			builder.withString("file", file);
    		}
    		
    		if (configuration.getDebugOutput()) {
    			System.out.println("Sending request to /asset/"+id);
    		}
    		
	    	Response<byte[]> response = searchClient.download(token, id);
	    	if (response.status() == 200) {
	    		try {
					Files.write(Paths.get(file), response.body(), StandardOpenOption.CREATE);
		   			builder.withString("result", "OK");
		   			defaultOutput.emit(builder.build());
				} catch (IOException e) {
		    		builder.withString("error", "Exception:" + e.getMessage());
		    		REJECTOutput.emit(builder.build());
				}
	    	} else {
	    		builder.withString("error", "Error:" + response.error(String.class));
	    		REJECTOutput.emit(builder.build());
	    	}
    	}
    }

	@AfterGroup
    public void afterGroup() {
    }
}