package com.bc.calvalus.wpsrest.wpsoperations;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;

/**
 * This is a builder to WpsMetadata object. This opens the possibilities of propagating
 * information from the Web Service interface to the back end.
 *
 * @author hans
 */
public class WpsMetadataBuilder {

    private ServletRequestWrapper servletRequestWrapper;

    private WpsMetadataBuilder() {
    }

    public static WpsMetadataBuilder create() {
        return new WpsMetadataBuilder();
    }

    public WpsMetadata build() {
        return new WpsMetadata(this);
    }

    public WpsMetadataBuilder withServletRequestWrapper(ServletRequestWrapper servletRequestWrapper) {
        this.servletRequestWrapper = servletRequestWrapper;
        return this;
    }

    public ServletRequestWrapper getServletRequestWrapper() {
        return servletRequestWrapper;
    }
}
