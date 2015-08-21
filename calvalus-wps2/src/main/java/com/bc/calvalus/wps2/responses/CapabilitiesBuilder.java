package com.bc.calvalus.wps2.responses;

import com.bc.calvalus.wps2.jaxb.Languages;
import com.bc.calvalus.wps2.jaxb.ProcessOfferings;
import com.bc.calvalus.wps2.jaxb.ServiceIdentification;
import com.bc.calvalus.wps2.jaxb.ServiceProvider;

/**
 * Created by hans on 17/08/2015.
 */
public class CapabilitiesBuilder {

    private ServiceIdentification serviceIdentification;
    private ServiceProvider serviceProvider;
    private ProcessOfferings processOfferings;
    private Languages languages;

    private CapabilitiesBuilder() {

    }

    public static CapabilitiesBuilder create() {
        return new CapabilitiesBuilder();
    }

    public GetCapabilitiesResponse build() {
        return new GetCapabilitiesResponse(this);
    }

    public CapabilitiesBuilder withServiceIdentification(ServiceIdentification serviceIdentification) {
        this.serviceIdentification = serviceIdentification;
        return this;
    }

    public CapabilitiesBuilder withServiceProvider(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
        return this;
    }

    public CapabilitiesBuilder withProcessOfferings(ProcessOfferings processOfferings) {
        this.processOfferings = processOfferings;
        return this;
    }

    public CapabilitiesBuilder withLanguages(Languages languages) {
        this.languages = languages;
        return this;
    }

    public ServiceIdentification getServiceIdentification() {
        return serviceIdentification;
    }

    public ServiceProvider getServiceProvider() {
        return serviceProvider;
    }

    public ProcessOfferings getProcessOfferings() {
        return processOfferings;
    }

    public Languages getLanguages() {
        return languages;
    }
}
