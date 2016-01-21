package com.bc.calvalus.wps.utils;

import javax.servlet.http.HttpServletRequest;

/**
 * @author hans
 */
public class ServletRequestWrapper {

    private final HttpServletRequest servletRequest;

    public ServletRequestWrapper(HttpServletRequest servletRequest) {
        this.servletRequest = servletRequest;
    }

    public String getUserName() {
        return servletRequest.getUserPrincipal().getName();
    }

    public String getServerName() {
        return servletRequest.getServerName();
    }

    public String getPortNumber() {
        return Integer.toString(servletRequest.getServerPort());
    }

    public String getRequestUrl() {
        return servletRequest.getRequestURL().toString();
    }
}
