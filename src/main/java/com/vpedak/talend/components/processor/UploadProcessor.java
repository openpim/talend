package com.vpedak.talend.components.processor;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

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
@Icon(value = CUSTOM, custom = "PIMAssetUpload") // icon is located at src/main/resources/icons/PIMAssetUpload.svg
@Processor(name = "AssetUpload")
@Documentation("TODO fill the documentation for this processor")
public class UploadProcessor implements Serializable {
    private static final String BOUNDARY = "--XzRWzrqGywOAdy4J-Jdf06LZhRC99B--";
	private final AssetsProcessorConfiguration configuration;
    private final RecordBuilderFactory builderFactory;
    private final SearchClient searchClient;
	private boolean isGuessSchema = false;
	private OutputGuessSchemaHelper schemaHelper;
	private String token;

    public UploadProcessor(@Option("configuration") final AssetsProcessorConfiguration configuration,
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
        	searchClient.base(configuration.getDatastore().getUrl().toString());
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
    		
    		File f = new File(file);
    		if (!f.exists()) {
    			builder.withString("error", "File '" + file + "' does not exists");
    			REJECTOutput.emit(builder.build());
    			return;
    		}
    		
    		try {
	    		String mimeType = Files.probeContentType(Paths.get(file));
	    		if (mimeType == null) mimeType = "application/octet-stream";
	    		MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create().setBoundary(BOUNDARY);
	    		entityBuilder.addBinaryBody("file", f, ContentType.create(mimeType), f.getName());
	    		entityBuilder.addTextBody("id", id);
	    		
	    		ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    		entityBuilder.build().writeTo(bos);
	    		if (configuration.getDebugOutput()) {
	    			System.out.println("Sending request to /asset-upload: id="+id+", file: "+file);
	    		}
	    		Response<String> response = searchClient.upload("multipart/form-data; boundary="+BOUNDARY, token, bos.toByteArray());
	    		
	    		if (response.status() == 200) {
	    			builder.withString("result", "OK");
	    			defaultOutput.emit(builder.build());
	    		} else {
	    			builder.withString("error", "Error:" + response.error(String.class));
	    			REJECTOutput.emit(builder.build());
	    		}
    		} catch (IOException e) {
    			builder.withString("error", "Exception: " + e.getMessage());
    			REJECTOutput.emit(builder.build());
    			return;
			}
    	}
    }

	@AfterGroup
    public void afterGroup() {
    }
}