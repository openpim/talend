package com.vpedak.talend.components.source;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import com.vpedak.talend.components.dataset.SelectDataset;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" })
})
@Documentation("TODO fill the documentation for this configuration")
public class PIMSelectMapperConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private SelectDataset dataset;

    public SelectDataset getDataset() {
        return dataset;
    }

    public PIMSelectMapperConfiguration setDataset(SelectDataset dataset) {
        this.dataset = dataset;
        return this;
    }
}