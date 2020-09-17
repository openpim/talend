package com.vpedak.talend.components.processor;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import com.vpedak.talend.components.dataset.InputDataset;
import com.vpedak.talend.components.dataset.InputDataset.EntityEnum;
import com.vpedak.talend.components.datastore.CustomDatastore;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "entity" }),
    @GridLayout.Row({ "importMode" }),
    @GridLayout.Row({ "errorProcessing" }),
    @GridLayout.Row({ "debugOutput" })
})
@Documentation("TODO fill the documentation for this configuration")
public class OutputProcessorConfiguration implements IRequestConfiguration, Serializable {
    public enum ImportMode {
    	CREATE_UPDATE,
    	CREATE_ONLY,
    	UPDATE_ONLY
    }

    public enum ErrorProcessing {
    	PROCESS_WARN,
    	WARN_REJECTED
    }
    
    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private CustomDatastore datastore;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private EntityEnum entity = EntityEnum.ITEM;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private ImportMode importMode = ImportMode.CREATE_UPDATE;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private ErrorProcessing errorProcessing = ErrorProcessing.PROCESS_WARN;
    
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean debugOutput = false;
    
    public CustomDatastore getDatastore() {
        return datastore;
    }

    public OutputProcessorConfiguration setDatastore(CustomDatastore datastore) {
        this.datastore = datastore;
        return this;
    }

    public EntityEnum getEntity() {
        return entity;
    }

    public OutputProcessorConfiguration setEntity(EntityEnum entity) {
        this.entity = entity;
        return this;
    }

    public ImportMode getImportMode() {
        return importMode;
    }

    public OutputProcessorConfiguration setImportMode(ImportMode importMode) {
        this.importMode = importMode;
        return this;
    }

    public ErrorProcessing getErrorProcessing() {
        return errorProcessing;
    }

    public OutputProcessorConfiguration setErrorProcessing(ErrorProcessing errorProcessing) {
        this.errorProcessing = errorProcessing;
        return this;
    }
    
    public boolean getDebugOutput() {
        return debugOutput;
    }

    public OutputProcessorConfiguration setDebugOutput(boolean debugOutput) {
        this.debugOutput = debugOutput;
        return this;
    }
}