package com.bc.calvalus.generator.options;


import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.bc.calvalus.generator.Constants;
import org.apache.commons.cli.CommandLine;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * @author muhammad.bc
 */
public class HandleOptionTest {

    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

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
        HandleOption handleOption = new HandleOption(new String[]{});
        assertThat(outputStream.toString().replaceAll("[\r\n]+", ""),is("Parameter most be specify, for more detail type 'Exec -h'"));

    }

    @Ignore // TODO: modify the test the reflect the change regarding file loading.
    @Test
    public void testStartCommand() throws Exception {
        PrintOption printOption = new HandleOption(new String[]{"-h"});
        assertNotNull(outputStream.toString());
        assertEquals(outputStream.toString().replaceAll("[\r\n]+", " "), Constants.HELP_INFO.replaceAll("[\r\n]+", " "));
    }

    @Test
    public void testOptionVersion() throws Exception {
        HandleOption printOption = new HandleOption(new String[]{"-v"});
        String version = PrintOption.getBuildProperties().getProperty("version");
        String actual = String.format("Calvalus Generator version %s.", version);
        assertEquals(outputStream.toString().replaceAll("[\r\n]+", ""), actual);
    }

    @Test
    public void testOptionParameter() throws Exception {
        HandleOption printOption = new HandleOption(new String[]{"start", "-i", "30", "-o", "c:/test",});
        CommandLine commandLine = printOption.getCommandLine();
        assertEquals(printOption.getOptionValue("i"), "30");
        assertEquals(printOption.getOptionValue("o"), "c:/test");
    }

    @Test
    public void testOptionWrongParameter() throws Exception {
        HandleOption printOption = new HandleOption(new String[]{"start", "-i", "30", "-o", "c:/test", "stop", "-f"});
        CommandLine commandLine = printOption.getCommandLine();
        assertNull(commandLine);
    }

}