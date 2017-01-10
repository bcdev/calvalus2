package com.bc.calvalus.reporting.ws;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author hans
 */
@ApplicationPath("/reporting")
public class ReportingApplication extends Application {

    private static final Set<Object> emptySet = Collections.emptySet();

    private Set<Class<?>> classes = new HashSet<>();

    @Override
    public Set<Class<?>> getClasses() {
        classes.add(ReportingService.class);
        return classes;
    }

    @Override
    public Set<Object> getSingletons() {
        return emptySet;
    }
}
