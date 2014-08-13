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
import com.bc.ceres.resource.StringResource;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ScriptGeneratorTest {

    @Test
    public void testCreateProcessedResourceName() throws Exception {
        assertEquals("cmdline", ScriptGenerator.createProcessedResourceName("l2gen-cmdline.vm", "l2gen"));
        assertEquals("config.txt", ScriptGenerator.createProcessedResourceName("l2gen-config.txt.vm", "l2gen"));
        assertEquals("config.txt", ScriptGenerator.createProcessedResourceName("l2gen-config.txt", "l2gen"));

        assertEquals("should-process-cmdline", ScriptGenerator.createProcessedResourceName("l2gen-should-process-cmdline.vm", "l2gen"));
        assertEquals("should-process.py", ScriptGenerator.createProcessedResourceName("l2gen-should-process.py", "l2gen"));

        assertEquals("routines.py", ScriptGenerator.createProcessedResourceName("common-routines.py", "foo"));
    }

    @Test
    public void testAddResource_WithoutVariables() throws Exception {
        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PROCESS, "foo");
        scriptGenerator.addResource(new StringResource("foo-script", "This is the script content"));
        Object[] keys = scriptGenerator.getVelocityContext().getKeys();
        assertEquals(1, keys.length);
        assertSame(String.class, keys[0].getClass());
        assertEquals("script", keys[0]);
        Resource scriptResource = (Resource) scriptGenerator.getVelocityContext().get("script");
        assertNotNull(scriptResource);
        assertEquals("This is the script content", scriptResource.getContent());
    }

    @Test
    public void testAddResource_WithVariableAndProcessing() throws Exception {
        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PROCESS, "foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-script.vm", "This is the $variable content"));
        Object[] keys = scriptGenerator.getVelocityContext().getKeys();
        assertEquals(2, keys.length);
        Resource scriptResource = (Resource) scriptGenerator.getVelocityContext().get("script");
        assertNotNull(scriptResource);
        assertEquals("This is the calvalus content", scriptResource.getContent());
    }

    /**
     * No processing because the file does not end with ".vm"
     */
    @Test
    public void testAddResource_WithVariableAndWithoutProcessing() throws Exception {
        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PROCESS, "foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-script", "This is the $variable content"));
        Object[] keys = scriptGenerator.getVelocityContext().getKeys();
        assertEquals(2, keys.length);
        Resource scriptResource = (Resource) scriptGenerator.getVelocityContext().get("script");
        assertNotNull(scriptResource);
        assertEquals("This is the $variable content", scriptResource.getContent());
    }

    @Test
    public void testWriteScriptFiles_WithVelocityProcessing() throws Exception {
        TracingScriptGenerator scriptGenerator = new TracingScriptGenerator("foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-script.vm", "This is the $variable content"));
        scriptGenerator.writeScriptFiles(new File("."));

        assertEquals(1, scriptGenerator.fileNames.size());
        assertEquals(1, scriptGenerator.resources.size());
        assertEquals("script", scriptGenerator.fileNames.get(0));
        assertEquals("foo-script.vm", scriptGenerator.resources.get(0).getPath());
        assertEquals("This is the calvalus content", scriptGenerator.resources.get(0).getContent());
    }

    /**
     * No Velocity processing because the file does not end with ".vm"
     */
    @Test
    public void testWriteScriptFiles_WithoutVelocityProcessing() throws Exception {
        TracingScriptGenerator scriptGenerator = new TracingScriptGenerator("foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-script", "This is the $variable content"));
        scriptGenerator.writeScriptFiles(new File("."));

        assertEquals(1, scriptGenerator.fileNames.size());
        assertEquals(1, scriptGenerator.resources.size());
        assertEquals("script", scriptGenerator.fileNames.get(0));
        assertEquals("foo-script", scriptGenerator.resources.get(0).getPath());
        assertEquals("This is the $variable content", scriptGenerator.resources.get(0).getContent());
    }

    @Test
    public void testWriteScriptFiles_Prepare() throws Exception {
        TracingScriptGenerator scriptGenerator = new TracingScriptGenerator(ScriptGenerator.Step.PREPARE, "foo");
        scriptGenerator.addResource(new StringResource("foo-prepare.vm", "This is the prepare script"));
        scriptGenerator.addResource(new StringResource("foo-process.vm", "This is the process script"));
        scriptGenerator.addResource(new StringResource("foo-finalize.vm", "This is the finalize script"));
        scriptGenerator.addResource(new StringResource("foo-misc.txt", "misc data"));
        scriptGenerator.writeScriptFiles(new File("."));

        assertEquals(1, scriptGenerator.fileNames.size());
        assertEquals(1, scriptGenerator.resources.size());
        assertEquals("prepare", scriptGenerator.fileNames.get(0));
        assertEquals("foo-prepare.vm", scriptGenerator.resources.get(0).getPath());
        assertEquals("This is the prepare script", scriptGenerator.resources.get(0).getContent());
    }

    @Test
    public void testWriteScriptFiles_Process() throws Exception {
        TracingScriptGenerator scriptGenerator = new TracingScriptGenerator(ScriptGenerator.Step.PROCESS, "foo");
        scriptGenerator.addResource(new StringResource("foo-prepare.vm", "This is the prepare script"));
        scriptGenerator.addResource(new StringResource("foo-process.vm", "This is the process script"));
        scriptGenerator.addResource(new StringResource("foo-finalize.vm", "This is the finalize script"));
        scriptGenerator.addResource(new StringResource("foo-misc.txt", "misc data"));
        scriptGenerator.writeScriptFiles(new File("."));

        assertEquals(2, scriptGenerator.fileNames.size());
        assertEquals(2, scriptGenerator.resources.size());

        assertEquals("process", scriptGenerator.fileNames.get(0));
        assertEquals("foo-process.vm", scriptGenerator.resources.get(0).getPath());
        assertEquals("This is the process script", scriptGenerator.resources.get(0).getContent());

        assertEquals("misc.txt", scriptGenerator.fileNames.get(1));
        assertEquals("foo-misc.txt", scriptGenerator.resources.get(1).getPath());
        assertEquals("misc data", scriptGenerator.resources.get(1).getContent());
    }

    @Test
    public void testWriteScriptFiles_Finalize() throws Exception {
        TracingScriptGenerator scriptGenerator = new TracingScriptGenerator(ScriptGenerator.Step.FINALIZE, "foo");
        scriptGenerator.addResource(new StringResource("foo-prepare.vm", "This is the prepare script"));
        scriptGenerator.addResource(new StringResource("foo-process.vm", "This is the process script"));
        scriptGenerator.addResource(new StringResource("foo-finalize.vm", "This is the finalize script"));
        scriptGenerator.addResource(new StringResource("foo-misc.txt", "misc data"));

        scriptGenerator.writeScriptFiles(new File("."));

        assertEquals(1, scriptGenerator.fileNames.size());
        assertEquals(1, scriptGenerator.resources.size());
        assertEquals("finalize", scriptGenerator.fileNames.get(0));
        assertEquals("foo-finalize.vm", scriptGenerator.resources.get(0).getPath());
        assertEquals("This is the finalize script", scriptGenerator.resources.get(0).getContent());
    }

    private static class TracingScriptGenerator extends ScriptGenerator {
        List<String> fileNames = new ArrayList<String>();
        List<Resource> resources = new ArrayList<Resource>();

        public TracingScriptGenerator(String executableName) {
            this(ScriptGenerator.Step.PROCESS, executableName);
        }

        public TracingScriptGenerator(ScriptGenerator.Step step, String executableName) {
            super(step, executableName);
        }

        @Override
        void writeScript(File scriptFile, Resource resource) throws IOException {
            fileNames.add(scriptFile.getName());
            resources.add(resource);
        }
    }
}
