package com.bc.calvalus.wpsrest;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by hans on 15/09/2015.
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
