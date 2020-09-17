package com.vpedak.talend.components.processor;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

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
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.source.InputGuessSchemaHelper;
import com.vpedak.talend.components.source.PIMInputMapperConfiguration;
import com.vpedak.talend.components.source.PIMInputSource;
import com.vpedak.talend.components.source.SearchClient;

@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = CUSTOM, custom = "PIMRowInput") // icon is located at src/main/resources/icons/PIMRowInput.svg
@Processor(name = "RowInput")
@Documentation("TODO fill the documentation for this processor")
public class RowInputProcessor implements Serializable {
	private final PIMInputMapperConfiguration configuration;
    private final RecordBuilderFactory builderFactory;
    private final SearchClient searchClient;
	private InputGuessSchemaHelper schemaHelper;
	private String where;
	private List<String> params = new ArrayList<String>();

    public RowInputProcessor(@Option("configuration") final PIMInputMapperConfiguration configuration,
    		final RecordBuilderFactory builderFactory,
            final SearchClient searchClient) {
        this.configuration = configuration;
        this.builderFactory = builderFactory;
        this.searchClient = searchClient;
        
        this.where = configuration.getDataset().getWhere();
        int idx = where.indexOf("#[");
        while (idx != -1) {
        	int idx2 = where.indexOf("]", idx);
        	if (idx2 != -1) {
        		String param = where.substring(idx+2, idx2);
        		params.add(param);
        		idx = where.indexOf("#[", idx2);
        	}
        }
        
        searchClient.base(configuration.getDataset().getDatastore().getUrl().toString()); 
    }

    @PostConstruct
    public void init() {
    }
    
    @BeforeGroup
    public void beforeGroup() {
    }

    @ElementListener
    public void onNext(
            @Input final Record in,
            @Output final OutputEmitter<Record> defaultOutput,
            @Output("REJECT") final OutputEmitter<Record> REJECTOutput) {
    	
    	String where = this.where;
    	for(String param : params) {
    		String val = in.getString(param);
    		if (val == null) val = "";
    		
    		where = where.replace("#[" + param + "]", val);
    	}
    	configuration.getDataset().setWhere(where);
    	
    	PIMInputSource inputSource = new PIMInputSource(configuration, builderFactory, searchClient);
    	inputSource.init();
    	inputSource.setInitRecord(in);
    	
    	Record rec = inputSource.next();
    	while(rec != null) {
    		defaultOutput.emit(rec);
    		rec = inputSource.next();
    	}
		defaultOutput.emit(null);
    }

	@AfterGroup
    public void afterGroup() {
    }
}