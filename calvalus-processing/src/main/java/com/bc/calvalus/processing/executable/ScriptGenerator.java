/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.executable;

import com.bc.ceres.resource.Resource;
import com.bc.ceres.resource.ResourceEngine;
import org.apache.velocity.VelocityContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates the script to call the executable processor.
 * Evaluates the given templates using Velocity.
 */
public class ScriptGenerator {

    private static final String VM_SUFFIX = ".vm";
    private static final String CMDLINE = "cmdline";
    private static final String WRAPPER = "wrapper";

    private final ResourceEngine resourceEngine;
    private final String executableName;
    private final List<String> processedResourceNames;

    public ScriptGenerator(String executableName) {
        this.executableName = executableName;
        this.resourceEngine = new ResourceEngine();
        this.processedResourceNames = new ArrayList<String>();
    }

    public VelocityContext getVelocityContext() {
        return resourceEngine.getVelocityContext();
    }

    public void addResource(Resource resource) {
        String path = resource.getPath();
        String name = new File(path).getName();
        String processedName = createProcessedResourceName(name, executableName);
        if (name.endsWith(VM_SUFFIX)) {
            resourceEngine.processAndAddResource(processedName, resource);
        } else {
            getVelocityContext().put(processedName, resource);
        }
        processedResourceNames.add(processedName);
    }

    public String getCommandLine() {
        if (processedResourceNames.contains(CMDLINE)) {
            return getCommandLineImpl(CMDLINE);
        } else if (processedResourceNames.contains(WRAPPER)) {
            return WRAPPER;
        }
        throw new NullPointerException("no 'cmdline' resource '" + CMDLINE + "' specified");
    }

    public String getCommandLine(String name) {
        if (processedResourceNames.contains(name + "-" + CMDLINE)) {
            return getCommandLineImpl(name + "-" + CMDLINE);
        } else if (processedResourceNames.contains(name + "-" + WRAPPER)) {
            return name + "-" + WRAPPER;
        }
        throw new NullPointerException("no 'cmdline' resource '" + name + "' specified");
    }

    private String getCommandLineImpl(String cmdlineName) {
        Resource resource = resourceEngine.getResource(cmdlineName);
        if (resource != null) {
            return resource.getContent();
        }
        return null;
    }

    public void writeScriptFiles(File cwd) throws IOException {
        for (String resourceName : processedResourceNames) {
            if (!resourceName.endsWith(CMDLINE)) {
                Resource resource = resourceEngine.getResource(resourceName);
                File scriptFile = new File(cwd, resourceName);
                writeScript(scriptFile, resource);
            }
        }
    }

    void writeScript(File scriptFile, Resource resource) throws IOException {
        System.out.println("scriptFile = " + scriptFile.getCanonicalPath());
        if (scriptFile.exists()) {
            scriptFile.delete();
        }
        Writer writer = new FileWriter(scriptFile);
        try {
            writer.write(resource.getContent());
        } finally {
            writer.close();
        }
        scriptFile.setExecutable(true);
    }

    static String createProcessedResourceName(String name, String executable) {
        name = name.substring(executable.length() + 1); // strip executable name from the front
        if (name.endsWith(VM_SUFFIX)) {
            name = name.substring(0, name.length() - VM_SUFFIX.length()); // strip .vm from the end
        }
        return name;
    }
}
