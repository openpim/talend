package com.vpedak.talend.components.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import com.vpedak.talend.components.datastore.SelectDatastore;
import com.vpedak.talend.components.processor.ISelectConfiguration;

@DataSet("SelectDataset")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "automaticLogin" }),
    @GridLayout.Row({ "username" }),
    @GridLayout.Row({ "password" })
})
@Documentation("TODO fill the documentation for this configuration")
public class SelectDataset implements ISelectConfiguration, Serializable {
	
    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private SelectDatastore datastore;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean automaticLogin = false;

    @Option
    @ActiveIf(target = "automaticLogin", value = { "true" })
    @Documentation("TODO fill the documentation for this parameter")
    private String username;

    @Option
    @Credential
    @ActiveIf(target = "automaticLogin", value = { "true" })
    @Documentation("TODO fill the documentation for this parameter")
    private String password;
    
    public SelectDatastore getDatastore() {
        return datastore;
    }

    public SelectDataset setDatastore(SelectDatastore datastore) {
        this.datastore = datastore;
        return this;
    }

    public boolean getAutomaticLogin() {
        return automaticLogin;
    }

    public SelectDataset setAutomaticLogin(boolean automaticLogin) {
        this.automaticLogin = automaticLogin;
        return this;
    }
    
    public String getUsername() {
        return username;
    }

    public SelectDataset setUsername(String username) {
        this.username = username;
        return this;
    }
    
    public String getPassword() {
        return password;
    }

    public SelectDataset setPassword(String password) {
        this.password = password;
        return this;
    }
}