package com.vpedak.talend.components.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.browser.WebBrowserFrame;

@Documentation("TODO fill the documentation for this source")
public class PIMSelectSource implements Serializable {
	Logger logger = Logger.getLogger("PIMInputSource");
	private final PIMSelectMapperConfiguration configuration;
	private final RecordBuilderFactory builderFactory;
	private boolean isGuessSchema = false;
	private Record initRecord = null;
	private Iterator<String> iterator = null;

	public PIMSelectSource(@Option("configuration") final PIMSelectMapperConfiguration configuration,
			final RecordBuilderFactory builderFactory) {
		this.configuration = configuration;
		this.builderFactory = builderFactory;
	}

	@PostConstruct
	public void init() {
		List<String> list = new ArrayList<String>();
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        String genisisClass = stackTraceElements[stackTraceElements.length -1].getClassName();
        if (genisisClass.contains("guess_schema"))
        {
            isGuessSchema = true;
            list.add("111");
        } else {
        	String url = configuration.getDataset().getDatastore().getUrl().toString()+"/#/export_login";
        	if (configuration.getDataset().getAutomaticLogin() && configuration.getDataset().getUsername() != null && !configuration.getDataset().getUsername().isEmpty() &&
        			configuration.getDataset().getPassword() != null && !configuration.getDataset().getPassword().isEmpty()	) {
        		url += "?user=" + configuration.getDataset().getUsername() + "&password=" + configuration.getDataset().getPassword();
        	}
        	WebBrowserFrame.start(url, list);
        }
        
        iterator = list.iterator();
	}

	@Producer
	public Record next() {
		Record.Builder builder = builderFactory.newRecordBuilder();
		if (iterator.hasNext()) {
			String str = iterator.next();
			builder.withString("identifier", str);
			return builder.build();
		} else {
			return null;
		}
	}

	public void setInitRecord(Record initRecord) {
		this.initRecord = initRecord;
	}
}