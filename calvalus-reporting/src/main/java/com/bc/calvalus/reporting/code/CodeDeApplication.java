package com.bc.calvalus.reporting.code;

import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

/**
 * @author muhammad.bc.
 */
@ApplicationPath("/code-de")
public class CodeDeApplication extends Application {
    Set<Class<?>> codeDeServices = new HashSet<>();

    @Override
    public Set<Class<?>> getClasses() {
        codeDeServices.add(CodeDeService.class);
        return codeDeServices;
    }
}
