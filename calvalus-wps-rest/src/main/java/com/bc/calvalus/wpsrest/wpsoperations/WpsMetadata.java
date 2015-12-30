package com.bc.calvalus.wpsrest.wpsoperations;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;

/**
 * This class contains web service metadata information that is required for
 * back end processing.
 *
 * @author hans
 */
public class WpsMetadata {

    private final ServletRequestWrapper servletRequestWrapper;

    public WpsMetadata(WpsMetadataBuilder builder) {
        this.servletRequestWrapper = builder.getServletRequestWrapper();
    }

    public ServletRequestWrapper getServletRequestWrapper() {
        return servletRequestWrapper;
    }
}
