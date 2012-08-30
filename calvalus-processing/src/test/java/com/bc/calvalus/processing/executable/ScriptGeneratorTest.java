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
    public void testCreateResultName() throws Exception {
        assertEquals("cmdline", ScriptGenerator.createProcessedName("l2gen-cmdline.vm", "l2gen"));
        assertEquals("config.txt", ScriptGenerator.createProcessedName("l2gen-config.txt.vm", "l2gen"));
        assertEquals("config.txt", ScriptGenerator.createProcessedName("l2gen-config.txt", "l2gen"));
    }

    @Test
    public void testAddResource_WithoutVariables() throws Exception {
        ScriptGenerator scriptGenerator = new ScriptGenerator("foo");
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
        ScriptGenerator scriptGenerator = new ScriptGenerator("foo");
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
        ScriptGenerator scriptGenerator = new ScriptGenerator("foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-script", "This is the $variable content"));
        Object[] keys = scriptGenerator.getVelocityContext().getKeys();
        assertEquals(2, keys.length);
        Resource scriptResource = (Resource) scriptGenerator.getVelocityContext().get("script");
        assertNotNull(scriptResource);
        assertEquals("This is the $variable content", scriptResource.getContent());
    }

    @Test
    public void testGetCommandline() throws Exception {
        ScriptGenerator scriptGenerator = new ScriptGenerator("foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-cmdline.vm", "This is the $variable content"));
        Object[] keys = scriptGenerator.getVelocityContext().getKeys();
        assertEquals(2, keys.length);
        assertEquals("This is the calvalus content", scriptGenerator.getCommandLine());
    }

    @Test(expected = NullPointerException.class)
    public void testGetCommandline_Failing() throws Exception {
        ScriptGenerator scriptGenerator = new ScriptGenerator("foo");
        scriptGenerator.getVelocityContext().put("variable", "calvalus");
        scriptGenerator.addResource(new StringResource("foo-file.vm", "This is the $variable content"));
        Object[] keys = scriptGenerator.getVelocityContext().getKeys();
        assertEquals(2, keys.length);
        assertEquals("This is the calvalus content", scriptGenerator.getCommandLine());
    }

    @Test
    public void testWriteScriptFiles_WithProcessing() throws Exception {
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
     * No processing because the file does not end with ".vm"
     */
    @Test
    public void testWriteScriptFiles_WithoutProcessing() throws Exception {
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


    private static class TracingScriptGenerator extends ScriptGenerator {
        List<String> fileNames = new ArrayList<String>();
        List<Resource> resources = new ArrayList<Resource>();

        public TracingScriptGenerator(String executableName) {
            super(executableName);
        }

        @Override
        void writeScript(File scriptFile, Resource resource) throws IOException {
            fileNames.add(scriptFile.getName());
            resources.add(resource);
        }
    }
}
