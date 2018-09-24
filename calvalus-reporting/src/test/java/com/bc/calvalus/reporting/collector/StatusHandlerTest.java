package com.bc.calvalus.reporting.collector;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.bc.calvalus.commons.util.PropertiesWrapper;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author hans
 */
public class StatusHandlerTest {

    private StatusHandler statusHandler;

    private Path statusFilePath;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("reporting-collector-test.properties");
        statusHandler = new StatusHandler();
        statusFilePath = Paths.get(PropertiesWrapper.get("name") + ".status");
    }

    @Test
    public void canInitReport() throws Exception {
        statusHandler.initReport(10);

        assertThat(Files.exists(statusFilePath), equalTo(true));

        Files.delete(statusFilePath);
        assertThat(Files.exists(statusFilePath), equalTo(false));
    }

    @Test
    public void canUpdateJobNumber() throws Exception {
        statusHandler.initReport(15);

        String[] elements = getStatusElements();
        assertThat(elements.length, equalTo(6));
        assertThat(elements[0], equalTo("0"));
        assertThat(elements[2], equalTo("15"));

        statusHandler.updateNewJobNumber(20);

        String[] elementsAfterUpdate = getStatusElements();
        assertThat(elementsAfterUpdate.length, equalTo(6));
        assertThat(elementsAfterUpdate[0], equalTo("5"));
        assertThat(elementsAfterUpdate[2], equalTo("15"));

        Files.delete(statusFilePath);
        assertThat(Files.exists(statusFilePath), equalTo(false));
    }

    @NotNull
    private String[] getStatusElements() throws IOException {
        String[] elements;
        try (BufferedReader in = new BufferedReader(new FileReader(statusFilePath.toFile()))) {
            String line = in.readLine();
            elements = line.split(" ");
        }
        return elements;
    }
}