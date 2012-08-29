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

import com.bc.ceres.resource.FileResource;
import com.bc.ceres.resource.Resource;
import com.bc.ceres.resource.ResourceEngine;
import com.bc.ceres.resource.StringResource;
import org.apache.velocity.VelocityContext;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
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

    private final ResourceEngine resourceEngine;
    private final String executable;
    private final List<String> resourceNames;

    public ScriptGenerator(String executable) {
        this.executable = executable;
        this.resourceEngine = new ResourceEngine();
        this.resourceNames = new ArrayList<String>();
    }

    public VelocityContext getVelocityContext() {
        return resourceEngine.getVelocityContext();
    }

    public void addResource(Resource resource) {
        String path = resource.getPath();
        System.out.println("path = " + path);
        String name = new File(path).getName();
        String resultName = createResultName(name, executable);
        System.out.println("resultName = " + resultName);
        resourceEngine.processAndAddResource(resultName, resource);
        resourceNames.add(resultName);
    }

    public String getCommandLine() {
        Resource resource = resourceEngine.getResource(CMDLINE);
        if (resource != null) {
            return resource.getContent();
        }
        return "bash";  // TODO
    }

    public Resource[] getProcessedResources() {
        List<Resource> resources = new ArrayList<Resource>(resourceNames.size());
        for (String resourceName : resourceNames) {
            if (!resourceName.equals(CMDLINE)) {
                Resource resource = resourceEngine.getResource(resourceName);
                resources.add(new StringResource(resourceName, resource.getContent()));
            }
        }
        return resources.toArray(new Resource[resources.size()]);
    }

    public void writeScripts(File cwd) throws IOException {
        Resource[] processedResources = getProcessedResources();
        for (Resource processedResource : processedResources) {
            String path = processedResource.getPath();
            File scriptFile = new File(cwd, path);
            System.out.println("scriptFile = " + scriptFile.getCanonicalPath());
            Writer writer = new FileWriter(scriptFile);
            try {
                writer.write(processedResource.getContent());
            } finally {
                writer.close();
            }
            scriptFile.setExecutable(true);
        }
    }

    static String createResultName(String name, String executable) {
        name = name.substring(executable.length() + 1); // strip executable name from the front
        if (name.endsWith(VM_SUFFIX)) {
            name = name.substring(0, name.length() - VM_SUFFIX.length()); // strip .vm from the end
        }
        return name;
    }
}
