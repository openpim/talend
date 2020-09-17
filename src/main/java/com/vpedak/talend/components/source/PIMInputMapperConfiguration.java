package com.vpedak.talend.components.source;

import java.io.Serializable;

import com.vpedak.talend.components.dataset.InputDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" })
})
@Documentation("TODO fill the documentation for this configuration")
public class PIMInputMapperConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private InputDataset dataset;

    public InputDataset getDataset() {
        return dataset;
    }

    public PIMInputMapperConfiguration setDataset(InputDataset dataset) {
        this.dataset = dataset;
        return this;
    }
}