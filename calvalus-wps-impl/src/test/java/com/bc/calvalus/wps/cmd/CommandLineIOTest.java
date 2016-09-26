package com.bc.calvalus.wps.cmd;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.*;

import java.util.List;

/**
 * @author hans
 */
public class CommandLineIOTest {

    private ByteArrayOutputStream mockOutputStream;
    private ByteArrayOutputStream mockErrorStream;

    private CommandLineIO commandLineIO;

    @Before
    public void setUp() throws Exception {
        mockOutputStream = mock(ByteArrayOutputStream.class);
        mockErrorStream = mock(ByteArrayOutputStream.class);
    }

    @Test
    public void canGetOutputStringList() throws Exception {
        when(mockOutputStream.toString()).thenReturn("Successfully added user test111 to LDAP\n" +
                                                     "Successfully set password for user test111");

        commandLineIO = new CommandLineIO(mockOutputStream, mockErrorStream);
        List<String> outputStringList = commandLineIO.getOutputStringList();

        assertThat(outputStringList.size(), equalTo(2));
        assertThat(outputStringList.get(0), equalTo("Successfully added user test111 to LDAP"));
        assertThat(outputStringList.get(1), equalTo("Successfully set password for user test111"));
    }

    @Test
    public void canGetErrorStringList() throws Exception {
        when(mockErrorStream.toString()).thenReturn("ERROR: user \"test111\" already exists\n" +
                                                    "Connection to auth closed.");

        commandLineIO = new CommandLineIO(mockOutputStream, mockErrorStream);
        List<String> errorStringList = commandLineIO.getErrorStringList();

        assertThat(errorStringList.size(), equalTo(2));
        assertThat(errorStringList.get(0), equalTo("ERROR: user \"test111\" already exists"));
        assertThat(errorStringList.get(1), equalTo("Connection to auth closed."));
    }

    @Test
    public void returnsEmptyStringWhenEmptyByteArrayOutputStream() throws Exception {
        commandLineIO = new CommandLineIO(new ByteArrayOutputStream(), new ByteArrayOutputStream());

        List<String> outputStringList = commandLineIO.getOutputStringList();
        List<String> errorStringList = commandLineIO.getErrorStringList();

        assertThat(outputStringList.size(), equalTo(1));
        assertThat(outputStringList.get(0), equalTo(""));
        assertThat(errorStringList.size(), equalTo(1));
        assertThat(errorStringList.get(0), equalTo(""));
    }
}