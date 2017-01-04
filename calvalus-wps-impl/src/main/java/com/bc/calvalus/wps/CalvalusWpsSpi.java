package com.bc.calvalus.wps;

import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.WpsServiceInstance;
import com.bc.wps.api.WpsServiceProvider;
import com.bc.wps.api.exceptions.WpsRuntimeException;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.IOException;

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
        try {
            PropertiesWrapper.loadConfigFile("calvalus-wps.properties");
        } catch (IOException exception) {
            throw new WpsRuntimeException("Unable to load calvalus-wps.properties file", exception);
        }
        return new CalvalusWpsProvider();
    }
}
