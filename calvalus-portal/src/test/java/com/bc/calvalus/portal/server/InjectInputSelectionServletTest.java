package com.bc.calvalus.portal.server;


import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.portal.shared.DtoDateRange;
import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gson.JsonSyntaxException;
import com.sun.security.auth.UserPrincipal;
import org.junit.*;
import org.junit.rules.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.stubbing.Answer;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class InjectInputSelectionServletTest {

    private HttpServletRequest mockRequest;
    private HttpServletResponse mockResponse;
    private ServletConfig mockServletConfig;
    private ServletContext mockServletContext;

    private InjectInputSelectionServlet injectServlet;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Before
    public void setUp() {
        mockResponse = mock(HttpServletResponse.class);
        mockServletConfig = mock(ServletConfig.class);
        mockServletContext = mock(ServletContext.class);
    }

    @Test
    public void canSetInputSelectionInServletContext() throws Exception {
        String completeJsonRequest = getCompleteJsonRequest();
        mockRequest = getMockHttpServletRequest(completeJsonRequest);

        when(mockServletContext.getAttribute("catalogueSearch_user1")).thenReturn(getTransformedInputSelection());
        when(mockServletConfig.getServletContext()).thenReturn(mockServletContext);

        injectServlet = new InjectInputSelectionServlet();
        injectServlet.init(mockServletConfig);
        injectServlet.doPost(mockRequest, mockResponse);

        ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DtoInputSelection> captor = ArgumentCaptor.forClass(DtoInputSelection.class);
        verify(mockServletContext, times(1)).setAttribute(stringCaptor.capture(), captor.capture());

        DtoInputSelection value = captor.getValue();
        assertThat(value, instanceOf(DtoInputSelection.class));
        assertThat(value.getCollectionName(), equalTo("Sentinel-2 L1C (Africa,Germany,lakes,cities)"));
        assertThat(value.getRegionGeometry(),
                   equalTo("POLYGON((75.8056640625 28.80226253886344,77.76123046875 28.80226253886344,77.76123046875 27.60171565966903,75.8056640625 27.60171565966903,75.8056640625 28.80226253886344))"));
        assertThat(value.getProductIdentifiers().size(), equalTo(5));
        assertThat(value.getProductIdentifiers().get(0),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LTN_20170919T080811"));
        assertThat(value.getProductIdentifiers().get(1),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LXP_20170919T080811"));
        assertThat(value.getProductIdentifiers().get(2),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36MZB_20170919T080811"));
        assertThat(value.getProductIdentifiers().get(3),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36MZE_20170919T080811"));
        assertThat(value.getProductIdentifiers().get(4),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36NXF_20170919T080811"));
        assertThat(value.getDateRange().getStartTime(), equalTo("2017-11-09T15:42:23.00Z"));
        assertThat(value.getDateRange().getEndTime(), equalTo("2017-11-10T15:42:23.00Z"));

        // to check if the attribute setting in the context is done properly
        Object objectFromContext = injectServlet.getServletContext().getAttribute("catalogueSearch_user1");
        assertThat(objectFromContext, instanceOf(DtoInputSelection.class));
        DtoInputSelection inputSelectionFromContext = (DtoInputSelection) objectFromContext;
        assertThat(inputSelectionFromContext.getCollectionName(), equalTo("dummyCollectionName"));
        List<String> productIdentifiers = inputSelectionFromContext.getProductIdentifiers();
        assertThat(productIdentifiers.size(), equalTo(2));
        assertThat(productIdentifiers.get(0), equalTo("productId1"));
        assertThat(productIdentifiers.get(1), equalTo("productId2"));
        assertThat(inputSelectionFromContext.getDateRange().getStartTime(), equalTo("2017-11-01T00:00:00.00Z"));
        assertThat(inputSelectionFromContext.getDateRange().getEndTime(), equalTo("2017-11-02T00:00:00.00Z"));
        assertThat(inputSelectionFromContext.getRegionGeometry(),
                   equalTo("POLYGON((-16.5234375 34.538237527295756,49.5703125 34.538237527295756,49.5703125 -38.052416771864834,-16.5234375 -38.052416771864834,-16.5234375 34.538237527295756))"));
    }

    @Test
    public void canSetInputSelectionWithPointInServletContext() throws Exception {
        String jsonRequest = getJsonRequestWithPoint();
        mockRequest = getMockHttpServletRequest(jsonRequest);

        when(mockServletContext.getAttribute("catalogueSearch_user1")).thenReturn(getTransformedInputSelection());
        when(mockServletConfig.getServletContext()).thenReturn(mockServletContext);

        injectServlet = new InjectInputSelectionServlet();
        injectServlet.init(mockServletConfig);
        injectServlet.doPost(mockRequest, mockResponse);

        ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DtoInputSelection> captor = ArgumentCaptor.forClass(DtoInputSelection.class);
        verify(mockServletContext, times(1)).setAttribute(stringCaptor.capture(), captor.capture());

        DtoInputSelection value = captor.getValue();
        assertThat(value, instanceOf(DtoInputSelection.class));
        assertThat(value.getCollectionName(), equalTo("Sentinel-2 SPONGE no geo-index"));
        assertThat(value.getRegionGeometry(),
                   equalTo("POLYGON((10.524795 51.439422, 10.524805 51.439422, 10.524805 51.439432, 10.524795 51.439432, 10.524795 51.439422))"));
        assertThat(value.getProductIdentifiers().size(), equalTo(2));
        assertThat(value.getProductIdentifiers().get(0),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LTN_20170919T080811"));
        assertThat(value.getProductIdentifiers().get(1),
                   equalTo("EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36NXF_20170919T080811"));
        assertThat(value.getDateRange().getStartTime(), equalTo("2015-06-05T00:00:00.00Z"));
        assertThat(value.getDateRange().getEndTime(), equalTo("2015-06-10T00:00:00.00Z"));
    }

    @Test
    public void canThrowExceptionWhenInvalidGeojson() throws Exception {
        String jsonRequest = getJsonRequestWithInvalidGeoJson();
        mockRequest = getMockHttpServletRequest(jsonRequest);

        when(mockServletContext.getAttribute("catalogueSearch_user1")).thenReturn(getTransformedInputSelection());
        when(mockServletConfig.getServletContext()).thenReturn(mockServletContext);

        thrownException.expect(JsonSyntaxException.class);
        thrownException.expectMessage("Expected name at line 11 column 6 path $.regionGeometry.type");

        injectServlet = new InjectInputSelectionServlet();
        injectServlet.init(mockServletConfig);
        injectServlet.doPost(mockRequest, mockResponse);
    }

    private HttpServletRequest getMockHttpServletRequest(String jsonRequest) throws IOException {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        ServletInputStream mockServletInputStream = getMockServletInputStream(jsonRequest);
        when(mockRequest.getInputStream()).thenReturn(mockServletInputStream);
        UserPrincipal mockUserPrincipal = new UserPrincipal("user1");
        when(mockRequest.getUserPrincipal()).thenReturn(mockUserPrincipal);
        return mockRequest;
    }

    private ServletInputStream getMockServletInputStream(String completeJsonRequest) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(completeJsonRequest.getBytes());
        ServletInputStream mockServletInputStream = mock(ServletInputStream.class);
        // the following code was obtained from http://blog.timmattison.com/archives/2014/12/16/mockito-and-servletinputstreams/
        when(mockServletInputStream.read(Matchers.any(), anyInt(), anyInt())).thenAnswer(
                    (Answer<Integer>) invocationOnMock -> {
                        Object[] args = invocationOnMock.getArguments();
                        byte[] output = (byte[]) args[0];
                        int offset = (int) args[1];
                        int length = (int) args[2];
                        return byteArrayInputStream.read(output, offset, length);
                    });
        return mockServletInputStream;
    }

    private DtoInputSelection getTransformedInputSelection() {
        List<String> productIdentifiers = new ArrayList<>();
        DtoDateRange dateRange = new DtoDateRange("2017-11-01T00:00:00.00Z", "2017-11-02T00:00:00.00Z");
        productIdentifiers.add("productId1");
        productIdentifiers.add("productId2");
        return new DtoInputSelection("dummyCollectionName",
                                     productIdentifiers,
                                     dateRange,
                                     "POLYGON((-16.5234375 34.538237527295756,49.5703125 34.538237527295756,49.5703125 -38.052416771864834,-16.5234375 -38.052416771864834,-16.5234375 34.538237527295756))"
        );
    }

    private String getJsonRequestWithInvalidGeoJson() {
        return "{\n" +
               "\t\"collectionName\": \"Sentinel-2 SPONGE no geo-index\",\n" +
               "\t\"productIdentifiers\": [\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LTN_20170919T080811\",\n" +
               "\t\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36NXF_20170919T080811\"],\n" +
               "\t\"dateRange\": {\n" +
               "\t\t\"startTime\": \"2015-06-05T00:00:00.00Z\",\n" +
               "\t\t\"endTime\": \"2015-06-10T00:00:00.00Z\"\n" +
               "\t},\n" +
               "\t\"regionGeometry\": {\n" +
               "      \"type\": \"Feature\",\n" +
               "    }\n" +
               "}";
    }

    private String getJsonRequestWithPoint() {
        return "{\n" +
               "\t\"collectionName\": \"Sentinel-2 SPONGE no geo-index\",\n" +
               "\t\"productIdentifiers\": [\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LTN_20170919T080811\",\n" +
               "\t\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36NXF_20170919T080811\"],\n" +
               "\t\"dateRange\": {\n" +
               "\t\t\"startTime\": \"2015-06-05T00:00:00.00Z\",\n" +
               "\t\t\"endTime\": \"2015-06-10T00:00:00.00Z\"\n" +
               "\t},\n" +
               "\t\"regionGeometry\": {\n" +
               "      \"type\": \"Feature\",\n" +
               "      \"geometry\": {\n" +
               "        \"type\": \"Point\",\n" +
               "             \"coordinates\": [10.52479963549, 51.43942678482501]\n" +
               "      },\n" +
               "      \"properties\": {\n" +
               "        \"name\": \"Bleicherode\"\n" +
               "      }\n" +
               "    }\n" +
               "}";
    }

    private String getCompleteJsonRequest() {
        return "{\n" +
               "\t\"collectionName\": \"Sentinel-2 L1C (Africa,Germany,lakes,cities)\",\n" +
               "\t\"productIdentifiers\": [\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LTN_20170919T080811\",\n" +
               "\t\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36LXP_20170919T080811\",\n" +
               "\t\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36MZB_20170919T080811\",\n" +
               "\t\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36MZE_20170919T080811\",\n" +
               "\t\"EOP:CODE-DE:S2_L1C:/S2A_MSIL1C_20170919T074611_N0205_R135_T36NXF_20170919T080811\"],\n" +
               "\t\"dateRange\": {\n" +
               "\t\t\"startTime\": \"2017-11-09T15:42:23.00Z\",\n" +
               "\t\t\"endTime\": \"2017-11-10T15:42:23.00Z\"\n" +
               "\t},\n" +
               "\t\"regionGeometry\": {\n" +
               "      \"type\": \"Feature\",\n" +
               "      \"geometry\": {\n" +
               "        \"type\": \"Polygon\",\n" +
               "             \"coordinates\": [\n" +
               "                 [\n" +
               "                     [75.8056640625, 28.80226253886344],\n" +
               "                     [77.76123046875, 28.80226253886344],\n" +
               "                     [77.76123046875, 27.60171565966903],\n" +
               "                     [75.8056640625, 27.60171565966903],\n" +
               "                     [75.8056640625, 28.80226253886344]\n" +
               "                 ]\n" +
               "             ]\n" +
               "      },\n" +
               "      \"properties\": {\n" +
               "        \"name\": \"New Delhi\"\n" +
               "      }\n" +
               "    }\n" +
               "}";
    }

}