package com.vpedak.talend.components.processor;

import com.vpedak.talend.components.datastore.CustomDatastore;

public interface IRequestConfiguration {
	public CustomDatastore getDatastore();
	public boolean getDebugOutput();
}
