package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.WpsException;

/**
 * @author hans
 */
public class WpsServiceFactory {

    private final ServletRequestWrapper servletRequestWrapper;

    public WpsServiceFactory(ServletRequestWrapper servletRequestWrapper) {
        this.servletRequestWrapper = servletRequestWrapper;
    }

    public AbstractWpsService getWpsService(String applicationName) {
        if ("calvalus".equalsIgnoreCase(applicationName)) {
            return new CalvalusWpsService(servletRequestWrapper);
        } else {
            throw new WpsException("Unknown application name '" + applicationName + "'.");
        }
    }
}
