package com.bc.calvalus.rest;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author hans
 */
@ApplicationPath("/")
public class CalvalusApplication extends Application {

    private static final Set<Object> emptySet = Collections.emptySet();

    private Set<Class<?>> classes = new HashSet<>();

    @Override
    public Set<Class<?>> getClasses() {
        classes.add(CalvalusService.class);
        classes.add(CORSFilter.class);
        return classes;
    }

    @Override
    public Set<Object> getSingletons() {
        return emptySet;
    }
}
