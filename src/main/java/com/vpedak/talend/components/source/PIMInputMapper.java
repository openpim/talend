package com.vpedak.talend.components.source;

import static java.util.Collections.singletonList;
import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;
import java.util.List;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.vpedak.talend.components.service.PimComponentsService;

//
// this class role is to enable the work to be distributed in environments supporting it.
//
@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = CUSTOM, custom = "PIMInput") // icon is located at src/main/resources/icons/PIMInput.svg
@PartitionMapper(name = "Input")
@Documentation("TODO fill the documentation for this mapper")
public class PIMInputMapper implements Serializable {
    private final PIMInputMapperConfiguration configuration;
    private final PimComponentsService service;
    private final RecordBuilderFactory recordBuilderFactory;
    private final SearchClient searchClient;

    public PIMInputMapper(@Option("configuration") final PIMInputMapperConfiguration configuration,
                        final PimComponentsService service,
                        final RecordBuilderFactory recordBuilderFactory,
                        final SearchClient searchClient) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.searchClient = searchClient;
    }

    @PostConstruct
    public void init() {
        searchClient.base(Utils.prepareUrl(configuration.getDataset().getDatastore().getUrl().toString())); 
    }
    
    @Assessor
    public long estimateSize() {
        // this method should return the estimation of the dataset size
        // it is recommended to return a byte value
        // if you don't have the exact size you can use a rough estimation
        return 1L;
    }

    @Split
    public List<PIMInputMapper> split(@PartitionSize final long bundles) {
        // overall idea here is to split the work related to configuration in bundles of size "bundles"
        //
        // for instance if your estimateSize() returned 1000 and you can run on 10 nodes
        // then the environment can decide to run it concurrently (10 * 100).
        // In this case bundles = 100 and we must try to return 10 PIMInputMapper with 1/10 of the overall work each.
        //
        // default implementation returns this which means it doesn't support the work to be split
        return singletonList(this);
    }

    @Emitter
    public PIMInputSource createWorker() {
        // here we create an actual worker,
        // you are free to rework the configuration etc but our default generated implementation
        // propagates the partition mapper entries.
        return new PIMInputSource(configuration, recordBuilderFactory, searchClient);
    }
}