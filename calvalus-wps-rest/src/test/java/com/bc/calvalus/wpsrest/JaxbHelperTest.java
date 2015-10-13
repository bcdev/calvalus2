package com.bc.calvalus.wpsrest;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.LanguageStringType;
import com.bc.calvalus.wpsrest.jaxb.ServiceIdentification;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Created by hans on 15/09/2015.
 */
public class JaxbHelperTest {

    /**
     * Class under test.
     */
    private JaxbHelper jaxbHelper;

    @Before
    public void setUp() throws Exception {
        jaxbHelper = new JaxbHelper();
    }

    @Test
    public void testMarshal() throws Exception {
        Capabilities capabilities = createCapabilities();
        StringWriter writer = new StringWriter();

        jaxbHelper.marshal(capabilities, writer);

        assertThat(writer.toString(), equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                              "<wps:Capabilities service=\"WPS\" xml:lang=\"en\" version=\"1.0.0\" xmlns:bc=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
                                              "    <wps:ServiceIdentification>\n" +
                                              "        <ows:Title>Calvalus WPS Server</ows:Title>\n" +
                                              "        <ows:Abstract>Web Processing Service for Calvalus</ows:Abstract>\n" +
                                              "        <wps:ServiceType>WPS</wps:ServiceType>\n" +
                                              "        <wps:ServiceTypeVersion>1.0.0</wps:ServiceTypeVersion>\n" +
                                              "    </wps:ServiceIdentification>\n" +
                                              "</wps:Capabilities>\n"));
    }

    @Test
    public void testUnmarshal() throws Exception {
        String executeResponseXmlString = getExecuteResponseXmlString();
        InputStream requestInputStream = new ByteArrayInputStream(executeResponseXmlString.getBytes());

        ExecuteResponse executeResponse = (ExecuteResponse) jaxbHelper.unmarshal(requestInputStream);

        assertThat(executeResponse.getVersion(), equalTo("1.0.0"));
        assertThat(executeResponse.getService(), equalTo("WPS"));
        assertThat(executeResponse.getLang(), equalTo("en"));
        assertThat(executeResponse.getStatusLocation(), equalTo("http://calvalustomcat:9080/calwps/wps?Service=WPS&Request=GetStatus&JobId=20150915093239_L3_14292f372703fc"));
        assertThat(executeResponse.getStatus().getProcessAccepted(), equalTo("The request has been accepted. The status of the process can be found in the URL."));
    }

    private String getExecuteResponseXmlString() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<wps:ExecuteResponse statusLocation=\"http://calvalustomcat:9080/calwps/wps?Service=WPS&amp;Request=GetStatus&amp;JobId=20150915093239_L3_14292f372703fc\" service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:bc=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "    <wps:Status creationTime=\"2015-09-15T09:32:42.225+02:00\">\n" +
               "        <wps:ProcessAccepted>The request has been accepted. The status of the process can be found in the URL.</wps:ProcessAccepted>\n" +
               "    </wps:Status>\n" +
               "</wps:ExecuteResponse>";
    }

    private Capabilities createCapabilities() {
        Capabilities capabilities = new Capabilities();
        capabilities.setLang("en");
        capabilities.setService("WPS");
        capabilities.setVersion("1.0.0");

        ServiceIdentification serviceIdentification = new ServiceIdentification();
        LanguageStringType title = new LanguageStringType();
        title.setValue("Calvalus WPS Server");
        serviceIdentification.setTitle(title);
        LanguageStringType abstractText = new LanguageStringType();
        abstractText.setValue("Web Processing Service for Calvalus");
        serviceIdentification.setAbstract(abstractText);
        CodeType serviceType = new CodeType();
        serviceType.setValue("WPS");
        serviceIdentification.setServiceType(serviceType);
        serviceIdentification.getServiceTypeVersion().add("1.0.0");
        capabilities.setServiceIdentification(serviceIdentification);

        return capabilities;
    }
}