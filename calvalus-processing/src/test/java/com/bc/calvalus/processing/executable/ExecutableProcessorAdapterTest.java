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


import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.StringReader;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

public class ExecutableProcessorAdapterTest {

    @Test
    public void testCreateResultName() throws Exception {
        assertEquals("cmdline", ExecutableProcessorAdapter.createResultName("l2gen-cmdline.vm", "l2gen"));
        assertEquals("config.txt", ExecutableProcessorAdapter.createResultName("l2gen-config.txt.vm", "l2gen"));
        assertEquals("config.txt", ExecutableProcessorAdapter.createResultName("l2gen-config.txt", "l2gen"));
    }

    @Test
    public void testFoo() throws Exception {
        ExecutableProcessorAdapter.TemplateProcessor templateProcessor = new ExecutableProcessorAdapter.TemplateProcessor();
        templateProcessor.velocityContext.put("system", System.getProperties());
        Configuration configuration = new Configuration();
        templateProcessor.velocityContext.put("configuration", configuration);
        StringWriter outputWriter = new StringWriter();
        StringReader inputReader = new StringReader("#foreach ($entry in $configuration.iterator())\n$entry.key ==> $entry.value\n#end\n");
        templateProcessor.velocityEngine.evaluate(templateProcessor.velocityContext, outputWriter, "ExecutableProcessorAdapter", inputReader);
        System.out.println(outputWriter.toString());

    }
}
