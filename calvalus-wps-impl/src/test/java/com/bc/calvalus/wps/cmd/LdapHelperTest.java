package com.bc.calvalus.wps.cmd;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.bc.calvalus.wps.exceptions.CommandLineException;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LdapHelper.class, CommandLineWrapperBuilder.class, CommandLineWrapper.class})
public class LdapHelperTest {

    private static final String MOCK_REMOTE_USER_NAME = "mockRemoteUserName";

    private CommandLineWrapper mockCmdWrapper;
    private CommandLineIO mockCmdIO;
    private CommandLineResultHandler mockResultHandler;

    private LdapHelper ldapHelper;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
    }

    @Test
    public void canReturnFalseWhenLdapCommandFails() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        when(mockResultHandler.getExitValue()).thenReturn(1);
        when(mockResultHandler.hasResult()).thenReturn(true);
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        boolean registered = ldapHelper.isRegistered(MOCK_REMOTE_USER_NAME);

        assertThat(registered, equalTo(false));
    }

    @Test(expected = CommandLineException.class)
    public void canThrowExceptionWhenCommandTimesOut() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        doThrow(new InterruptedException()).when(mockResultHandler).waitFor(anyLong());
        when(mockResultHandler.hasResult()).thenReturn(true);
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        ldapHelper.isRegistered(MOCK_REMOTE_USER_NAME);
    }

    @Test
    public void canReturnFalseWhenUserNotInAllowedGroup() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        when(mockResultHandler.getExitValue()).thenReturn(0);
        when(mockResultHandler.hasResult()).thenReturn(true);
        List<String> mockOutputStringList = new ArrayList<>();
        mockOutputStringList.add("uid=10224(tep_hans) gid=10118(calwps) groups=10118(dummy)");
        mockOutputStringList.add("Connection to auth closed.");
        when(mockCmdIO.getOutputStringList()).thenReturn(mockOutputStringList);
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        boolean registered = ldapHelper.isRegistered(MOCK_REMOTE_USER_NAME);

        assertThat(registered, equalTo(false));
    }

    @Test
    public void canReturnTrue() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        when(mockResultHandler.getExitValue()).thenReturn(0);
        when(mockResultHandler.hasResult()).thenReturn(true);
        List<String> mockOutputStringList = new ArrayList<>();
        mockOutputStringList.add("uid=10224(tep_hans) gid=10118(calwps) groups=10118(calwps)");
        mockOutputStringList.add("Connection to auth closed.");
        when(mockCmdIO.getOutputStringList()).thenReturn(mockOutputStringList);
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        boolean registered = ldapHelper.isRegistered(MOCK_REMOTE_USER_NAME);

        assertThat(registered, equalTo(true));
    }

    @Test
    public void canRegisterRemoteUser() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        when(mockResultHandler.getExitValue()).thenReturn(0);
        when(mockResultHandler.hasResult()).thenReturn(true);
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        ldapHelper.register(MOCK_REMOTE_USER_NAME);
    }

    @Test(expected = CommandLineException.class)
    public void canThrowExceptionWhenRegisteringProcessTimesOut() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        doThrow(new InterruptedException()).when(mockResultHandler).waitFor(anyLong());
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        ldapHelper.register(MOCK_REMOTE_USER_NAME);
    }

    @Test(expected = CommandLineException.class)
    public void canThrowExceptionWhenRegisteringProcessFails() throws Exception {
        mockCmdWrapper = mock(CommandLineWrapper.class);
        mockCmdIO = mock(CommandLineIO.class);
        mockResultHandler = mock(CommandLineResultHandler.class);
        when(mockResultHandler.getExitValue()).thenReturn(1);
        when(mockCmdIO.getResultHandler()).thenReturn(mockResultHandler);
        ByteArrayOutputStream mockOutputStream = mock(ByteArrayOutputStream.class);
        when(mockOutputStream.toString()).thenReturn("ERROR: user \"test111\" already exists\n" +
                                                     "Connection to auth closed.");
        when(mockCmdIO.getOutputStream()).thenReturn(mockOutputStream);
        when(mockCmdWrapper.executeAsync()).thenReturn(mockCmdIO);
        whenNew(CommandLineWrapper.class).withAnyArguments().thenReturn(mockCmdWrapper);

        ldapHelper = new LdapHelper();
        ldapHelper.register(MOCK_REMOTE_USER_NAME);
    }

    @Test
    public void parseLdapResponseTest() {
        ArrayList<String> response = new ArrayList<>();
        response.add("uid=10230(tep_amarin) gid=10118(calwps) groups=10118(calwps),20009(tep_coreteam)\r");
        List<String> groups = new LdapHelper().parseLdapIdResponse(response);
        assertEquals("calwps", groups.get(0));
        assertEquals("tep_coreteam", groups.get(1));
        response.add("uid=10272(tep_arose) gid=20009(tep_coreteam) groups=10118(calwps),20009(tep_coreteam)");
        groups = new LdapHelper().parseLdapIdResponse(response);
        assertEquals("calwps", groups.get(0));
        assertEquals("tep_coreteam", groups.get(1));
    }
}