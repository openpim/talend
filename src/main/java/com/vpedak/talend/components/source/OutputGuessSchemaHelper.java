package com.vpedak.talend.components.source;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class OutputGuessSchemaHelper {
	private final RecordBuilderFactory builderFactory;
	private Record record;
	private boolean assets = false;

	public OutputGuessSchemaHelper(RecordBuilderFactory builderFactory) {
		this.builderFactory = builderFactory;
	}

	public OutputGuessSchemaHelper(RecordBuilderFactory builderFactory, boolean assets) {
		this.builderFactory = builderFactory;
		this.assets = assets;
	}
	
	private void initRecord() {
		Record.Builder builder = builderFactory.newRecordBuilder();

		builder.withString("id", "");
		builder.withString("result", "");
		if (assets) {
			builder.withString("file", "");
			builder.withString("error", "");
		} else {
			builder.withString("identifier", "");
			builder.withString("warning1", "");
			builder.withString("warning2", "");
			builder.withString("warning3", "");
			builder.withString("error1", "");
			builder.withString("error2", "");
			builder.withString("error3", "");
		}
		
		record = builder.build();
	}
	
	public Record next() {
		if (record == null) {
			initRecord();
			return record;
		} else {
			return null;
		}
	}
}
