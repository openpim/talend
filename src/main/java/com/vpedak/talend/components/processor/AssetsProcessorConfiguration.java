package com.vpedak.talend.components.processor;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import com.vpedak.talend.components.datastore.CustomDatastore;

@GridLayout({
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "debugOutput" })
})
@Documentation("TODO fill the documentation for this configuration")
public class AssetsProcessorConfiguration implements IRequestConfiguration, Serializable {
    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private CustomDatastore datastore;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean debugOutput = false;
    
    public CustomDatastore getDatastore() {
        return datastore;
    }

    public AssetsProcessorConfiguration setDatastore(CustomDatastore datastore) {
        this.datastore = datastore;
        return this;
    }
    
    public boolean getDebugOutput() {
        return debugOutput;
    }

    public AssetsProcessorConfiguration setDebugOutput(boolean debugOutput) {
        this.debugOutput = debugOutput;
        return this;
    }
}