package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.WpsRuntimeException;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadataBuilder;

/**
 * @author hans
 */
public class WpsServiceFactory {

    private final ServletRequestWrapper servletRequestWrapper;

    public WpsServiceFactory(ServletRequestWrapper servletRequestWrapper) {
        this.servletRequestWrapper = servletRequestWrapper;
    }

    public WpsServiceProvider getWpsService(String applicationName) {
        if ("calvalus".equalsIgnoreCase(applicationName)) {
            WpsMetadata calvalusWpsMetadata = WpsMetadataBuilder.create()
                        .withServletRequestWrapper(servletRequestWrapper)
                        .build();
            return new CalvalusWpsService(calvalusWpsMetadata);
        } else {
            throw new WpsRuntimeException("Unknown application name '" + applicationName + "'.");
        }
    }
}
