package com.bc.calvalus.generator.options;


import com.bc.wps.utilities.PropertiesWrapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * @author muhammad.bc
 */
public class CLIHandlerOptionTest {

    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    @Before
    public void setUp() throws Exception {
        System.setOut(new PrintStream(outputStream, true, "UTF-8"));
    }

    @After
    public void tearDown() throws Exception {
        outputStream.close();
    }

    @Test
    public void testNonParameter() throws Exception {
        CLIHandlerOption CLIHandlerOption = new CLIHandlerOption(new String[]{});
        assertThat(outputStream.toString().replaceAll("[\r\n]+", ""), is("Please specify a parameter, for more detail type '-h'"));

    }

    @Test
    public void testOptionVersion() throws Exception {
        CLIHandlerOption printOption = new CLIHandlerOption(new String[]{"-v"});
        String version = PropertiesWrapper.get("version");
        String actual = String.format("Calvalus Generator version %s.", version);
        assertEquals(outputStream.toString().replaceAll("[\r\n]+", ""), actual);
    }

    @Test
    public void testOptionParameter() throws Exception {
        CLIHandlerOption printOption = new CLIHandlerOption(new String[]{"start", "-i", "30", "-o", "c:/test",});
        CommandLine commandLine = printOption.getCommandLine();
        assertEquals("30", printOption.getOptionValue("i"));
        assertEquals("c:/test", printOption.getOptionValue("o"));
    }

    @Test
    public void testOptionWrongParameter() throws Exception {
        CLIHandlerOption printOption = new CLIHandlerOption(new String[]{"start", "-z", "30", "-o", "c:/test"});
        CommandLine commandLine = printOption.getCommandLine();
        assertNull(commandLine);
    }

}