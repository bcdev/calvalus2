package com.bc.calvalus.reporting.ws;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * @author hans
 */
@ApplicationPath("/reporting")
public class ReportingApplication extends Application {

    private Set<Class<?>> classes = new HashSet<>();

    @Override
    public Set<Class<?>> getClasses() {
        classes.add(ReportingService.class);
        return classes;
    }
}
