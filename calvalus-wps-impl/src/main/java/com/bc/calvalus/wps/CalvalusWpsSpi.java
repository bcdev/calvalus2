package com.bc.calvalus.wps;

import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.WpsServiceInstance;
import com.bc.wps.api.WpsServiceProvider;

/**
 * @author hans
 */
public class CalvalusWpsSpi implements WpsServiceProvider {

    @Override
    public String getId() {
        return "calvalus";
    }

    @Override
    public String getName() {
        return "Calvalus WPS Server";
    }

    @Override
    public String getDescription() {
        return "This is a Calvalus WPS implementation";
    }

    @Override
    public WpsServiceInstance createServiceInstance(WpsServerContext wpsServerContext) {
        return new CalvalusWpsProvider();
    }
}
