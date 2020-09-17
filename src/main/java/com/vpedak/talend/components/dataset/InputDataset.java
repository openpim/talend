package com.vpedak.talend.components.dataset;

import java.io.Serializable;

import com.vpedak.talend.components.datastore.CustomDatastore;
import com.vpedak.talend.components.processor.IRequestConfiguration;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

@DataSet("InputDataset")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "entity" }),
    @GridLayout.Row({ "where" }),
    @GridLayout.Row({ "order" }),
    @GridLayout.Row({ "pageSize" }),
    @GridLayout.Row({ "debugOutput" })
})
@Documentation("TODO fill the documentation for this configuration")
public class InputDataset implements IRequestConfiguration, Serializable {
    public enum EntityEnum {
    	ITEM,
    	ITEM_RELATION,
    	TYPE,
    	ATTRIBUTE,
    	ATTRIBUTE_GROUP,
    	RELATION,
    	USER,
    	ROLE,
    	LOV
    }
	
    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private CustomDatastore datastore;

    @Option
    @Required
    @DefaultValue("ITEM")
    @Documentation("TODO fill the documentation for this parameter")
    private EntityEnum entity = EntityEnum.ITEM;

    @Option
    @TextArea
    @Documentation("TODO fill the documentation for this parameter")
    private String where;

    @Option
    @TextArea
    @Documentation("TODO fill the documentation for this parameter")
    private String order;

    @Option
    @Required
    @DefaultValue("1000")
    @Documentation("TODO fill the documentation for this parameter")
    private int pageSize = 1000;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean debugOutput = false;
    
    public CustomDatastore getDatastore() {
        return datastore;
    }

    public InputDataset setDatastore(CustomDatastore datastore) {
        this.datastore = datastore;
        return this;
    }

    public EntityEnum getEntity() {
        return entity;
    }

    public InputDataset setEntity(EntityEnum entity) {
        this.entity = entity;
        return this;
    }

    public String getWhere() {
        return where;
    }

    public InputDataset setWhere(String where) {
        this.where = where;
        return this;
    }

    public String getOrder() {
        return order;
    }

    public InputDataset setOrder(String order) {
        this.order = order;
        return this;
    }

    public int getPageSize() {
        return pageSize;
    }

    public InputDataset setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }
    
    public boolean getDebugOutput() {
        return debugOutput;
    }

    public InputDataset setDebugOutput(boolean debugOutput) {
        this.debugOutput = debugOutput;
        return this;
    }
}