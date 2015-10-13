package com.bc.calvalus.wpsrest;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import org.junit.*;

import javax.servlet.http.HttpServletRequest;
import java.security.Principal;

/**
 * Created by hans on 15/09/2015.
 */
public class ServletRequestWrapperTest {

    HttpServletRequest mockHttpServletRequest;

    /**
     * Class under test.
     */
    private ServletRequestWrapper servletRequestWrapper;

    @Before
    public void setUp() throws Exception {
        mockHttpServletRequest = mock(HttpServletRequest.class);
    }

    @Test
    public void canGetUserName() throws Exception {
        Principal mockPrincipal = mock(Principal.class);
        when(mockPrincipal.getName()).thenReturn("dummyUserName");
        when(mockHttpServletRequest.getUserPrincipal()).thenReturn(mockPrincipal);

        servletRequestWrapper = new ServletRequestWrapper(mockHttpServletRequest);

        assertThat(servletRequestWrapper.getUserName(), equalTo("dummyUserName"));
    }

    @Test
    public void canGetServerName() throws Exception {
        when(mockHttpServletRequest.getServerName()).thenReturn("calvalustomcat");

        servletRequestWrapper = new ServletRequestWrapper(mockHttpServletRequest);

        assertThat(servletRequestWrapper.getServerName(), equalTo("calvalustomcat"));
    }

    @Test
    public void testGetPortNumber() throws Exception {
        when(mockHttpServletRequest.getServerPort()).thenReturn(8080);

        servletRequestWrapper = new ServletRequestWrapper(mockHttpServletRequest);

        assertThat(servletRequestWrapper.getPortNumber(), equalTo("8080"));
    }

    @Test
    public void testGetRequestUrl() throws Exception {
        StringBuffer stringBuffer = new StringBuffer("http://localhost:9080/calwps/wps");
        when(mockHttpServletRequest.getRequestURL()).thenReturn(stringBuffer);

        servletRequestWrapper = new ServletRequestWrapper(mockHttpServletRequest);

        assertThat(servletRequestWrapper.getRequestUrl(), equalTo("http://localhost:9080/calwps/wps"));
    }
}