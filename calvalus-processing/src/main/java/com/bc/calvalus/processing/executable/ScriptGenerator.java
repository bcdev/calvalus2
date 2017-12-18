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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.ceres.resource.ReaderResource;
import com.bc.ceres.resource.Resource;
import com.bc.ceres.resource.ResourceEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.velocity.VelocityContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

/**
 * Generates the step to call the executable processor.
 * Evaluates the given templates using Velocity.
 */
public class ScriptGenerator {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public enum Step {
        PREPARE {
            @Override
            boolean shouldWriteResource(String resourceName) {
                return resourceName.equals("prepare");
            }
        },
        PROCESS {
            @Override
            boolean shouldWriteResource(String resourceName) {
                return !resourceName.equals("prepare") && !resourceName.equals("finalize");
            }
        },
        FINALIZE {
            @Override
            boolean shouldWriteResource(String resourceName) {
                return resourceName.equals("finalize");
            }
        };

        abstract boolean shouldWriteResource(String resourceName);

    }

    private static final String VM_SUFFIX = ".vm";
    private final ResourceEngine resourceEngine;
    private final Step step;
    private final String executableName;
    private final List<String> processedResourceNames;

    public ScriptGenerator(Step step, String executableName) {
        this.step = step;
        this.executableName = executableName;
        this.resourceEngine = new ResourceEngine();
        this.processedResourceNames = new ArrayList<>();
    }

    public VelocityContext getVelocityContext() {
        return resourceEngine.getVelocityContext();
    }

    public void addResource(Resource resource) {
        String path = resource.getPath();
        String name = new File(path).getName();
        CalvalusLogger.getLogger().info("Adding resource " + name);
        String processedName = createProcessedResourceName(name, executableName);
        if (name.endsWith(VM_SUFFIX)) {
            resourceEngine.processAndAddResource(processedName, resource);
        } else {
            getVelocityContext().put(processedName, resource);
        }
        processedResourceNames.add(processedName);
    }

    public boolean hasStepScript() {
        return processedResourceNames.contains(step.toString().toLowerCase());
    }

    public void writeScriptFiles(File cwd, boolean debug) throws IOException {
        if (debug) {
            LOG.info("writing files for step = " + step);
        }
        for (String resourceName : processedResourceNames) {
            if (step.shouldWriteResource(resourceName)) {
                Resource resource = resourceEngine.getResource(resourceName);
                File scriptFile = new File(cwd, resourceName);
                writeScript(scriptFile, resource, debug);
            }
        }
    }

    public void addScriptResources(Configuration conf, String parameterSuffix) throws IOException {
        Collection<String> scriptFiles = conf.getStringCollection(ProcessorFactory.CALVALUS_L2_PROCESSOR_FILES + parameterSuffix);
        for (String scriptFile : scriptFiles) {
            Path scriptFilePath = new Path(scriptFile);
            InputStream inputStream = scriptFilePath.getFileSystem(conf).open(scriptFilePath);
            Reader reader = new BufferedReader(new InputStreamReader(inputStream));
            addResource(new ReaderResource(scriptFile, reader));
        }
    }

    void writeScript(File scriptFile, Resource resource, boolean debug) throws IOException {
        if (debug) {
            LOG.info("writeScript = " + scriptFile.getCanonicalPath());
        }
        if (scriptFile.exists()) {
            boolean deleted = scriptFile.delete();
            if (!deleted) {
                System.out.println("Failed to delete existing script file");
            }
        }
        try (Writer writer = new FileWriter(scriptFile)) {
            String resourceContent = resource.getContent();
            if (debug && resource.getPath().endsWith(VM_SUFFIX)) {
                LOG.info("===========================================");
                LOG.info(resourceContent);
                LOG.info("===========================================");
            }
            writer.write(resourceContent);
        }
        boolean permissionChanged = scriptFile.setExecutable(true);
        if (!permissionChanged) {
            System.out.println("Failed to make script file executable");
        }
    }

    static String createProcessedResourceName(String name, String executable) {
        if (name.startsWith("common-")) {
            return createProcessedResourceNameImpl(name, "common");
        } else {
            return createProcessedResourceNameImpl(name, executable);
        }
    }

    private static String createProcessedResourceNameImpl(String name, String executable) {
        name = name.substring(executable.length() + 1); // strip executable name from the front
        if (name.endsWith(VM_SUFFIX)) {
            name = name.substring(0, name.length() - VM_SUFFIX.length()); // strip .vm from the end
        }
        return name;
    }
}
