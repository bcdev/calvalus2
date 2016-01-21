package com.bc.calvalus.wps.responses;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.wps.api.schema.Capabilities;
import com.bc.wps.api.schema.Languages;
import com.bc.wps.api.schema.OperationsMetadata;
import com.bc.wps.api.schema.ProcessOfferings;
import com.bc.wps.api.schema.ServiceIdentification;
import com.bc.wps.api.schema.ServiceProvider;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusGetCapabilitiesResponseConverterTest {

    private CalvalusGetCapabilitiesResponseConverter getCapabilitiesResponse;

    @Before
    public void setUp() throws Exception {
        getCapabilitiesResponse = new CalvalusGetCapabilitiesResponseConverter();
    }

    @Test
    public void canGetCapabilities() throws Exception {
        List<IWpsProcess> mockProcessList = new ArrayList<>();

        IWpsProcess process1 = mock(IWpsProcess.class);
        when(process1.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process1.getTitle()).thenReturn("Band arythmetic processor");
        when(process1.getAbstractText()).thenReturn("Some description");

        IWpsProcess process2 = mock(IWpsProcess.class);
        when(process2.getIdentifier()).thenReturn("beam-buildin~1.0~urban-tep-indices");
        when(process2.getTitle()).thenReturn("Urban TEP seasonality indices from MERIS SR");
        when(process2.getAbstractText()).thenReturn("Some description");

        mockProcessList.add(process1);
        mockProcessList.add(process2);

        Capabilities capabilities = getCapabilitiesResponse.constructGetCapabilitiesResponse(mockProcessList);

        assertThat(capabilities.getOperationsMetadata().getOperation().size(),equalTo(4));
        assertThat(capabilities.getServiceProvider().getProviderName(), equalTo("Brockmann-Consult"));
        assertThat(capabilities.getProcessOfferings().getProcess().size(), equalTo(2));
        assertThat(capabilities.getServiceIdentification().getTitle().getValue(), equalTo("Calvalus WPS server"));
        assertThat(capabilities.getLanguages().getDefault().getLanguage(), equalTo("EN"));

    }

    @Test
    public void canGetOperationsMetadata() throws Exception {
        OperationsMetadata operationsMetadata = getCapabilitiesResponse.getOperationsMetadata();

        assertThat(operationsMetadata.getOperation().size(), equalTo(4));
        assertThat(operationsMetadata.getOperation().get(0).getName(), equalTo("GetCapabilities"));
        assertThat(operationsMetadata.getOperation().get(1).getName(), equalTo("DescribeProcess"));
        assertThat(operationsMetadata.getOperation().get(2).getName(), equalTo("Execute"));
        assertThat(operationsMetadata.getOperation().get(3).getName(), equalTo("GetStatus"));
    }

    @Test
    public void canGetServiceProvider() throws Exception {
        ServiceProvider serviceProvider = getCapabilitiesResponse.getServiceProvider();

        assertThat(serviceProvider.getProviderName(), equalTo("Brockmann-Consult"));
        assertThat(serviceProvider.getProviderSite().getHref(), equalTo("http://www.brockmann-consult.de"));

        assertThat(serviceProvider.getServiceContact().getRole().getValue(), equalTo("PointOfContact"));
        assertThat(serviceProvider.getServiceContact().getIndividualName(), equalTo("Dr. Martin Boettcher"));
        assertThat(serviceProvider.getServiceContact().getPositionName(), equalTo("Project Manager"));

        assertThat(serviceProvider.getServiceContact().getContactInfo().getPhone().getVoice().size(), equalTo(1));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getPhone().getVoice().get(0), equalTo("+49 4152 889300"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getPhone().getFacsimile().size(), equalTo(1));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getPhone().getFacsimile().get(0), equalTo("+49 4152 889333"));

        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getDeliveryPoint().get(0), equalTo("Max-Planck-Str. 2"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getCity(), equalTo("Geesthacht"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getAdministrativeArea(), equalTo("SH"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getPostalCode(), equalTo("21502"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getCountry(), equalTo("Germany"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getElectronicMailAddress().size(), equalTo(1));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getAddress().getElectronicMailAddress().get(0), equalTo("info@brockmann-consult.de"));

        assertThat(serviceProvider.getServiceContact().getContactInfo().getHoursOfService(), equalTo("24x7"));
        assertThat(serviceProvider.getServiceContact().getContactInfo().getContactInstructions(), equalTo("Don't hesitate to call"));
    }

    @Test
    public void canGetProcessOfferings() throws Exception {
        List<IWpsProcess> mockProcessList = new ArrayList<>();

        IWpsProcess process1 = mock(IWpsProcess.class);
        when(process1.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process1.getTitle()).thenReturn("Band arythmetic processor");
        when(process1.getAbstractText()).thenReturn("Some description");

        IWpsProcess process2 = mock(IWpsProcess.class);
        when(process2.getIdentifier()).thenReturn("beam-buildin~1.0~urban-tep-indices");
        when(process2.getTitle()).thenReturn("Urban TEP seasonality indices from MERIS SR");
        when(process2.getAbstractText()).thenReturn("Some description");

        mockProcessList.add(process1);
        mockProcessList.add(process2);

        ProcessOfferings processOfferings = getCapabilitiesResponse.getProcessOfferings(mockProcessList);

        assertThat(processOfferings.getProcess().size(), equalTo(2));
        assertThat(processOfferings.getProcess().get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processOfferings.getProcess().get(0).getTitle().getValue(), equalTo("Band arythmetic processor"));
        assertThat(processOfferings.getProcess().get(0).getAbstract().getValue(), equalTo("Some description"));

        assertThat(processOfferings.getProcess().get(1).getIdentifier().getValue(), equalTo("beam-buildin~1.0~urban-tep-indices"));
        assertThat(processOfferings.getProcess().get(1).getTitle().getValue(), equalTo("Urban TEP seasonality indices from MERIS SR"));
        assertThat(processOfferings.getProcess().get(1).getAbstract().getValue(), equalTo("Some description"));

    }

    @Test
    public void canGetServiceIdentification() throws Exception {
        ServiceIdentification serviceIdentification = getCapabilitiesResponse.getServiceIdentification();

        assertThat(serviceIdentification.getTitle().getValue(), equalTo("Calvalus WPS server"));
        assertThat(serviceIdentification.getAbstract().getValue(), equalTo("Web Processing Service for Calvalus"));
        assertThat(serviceIdentification.getServiceType().getValue(), equalTo("WPS"));
        assertThat(serviceIdentification.getServiceTypeVersion().size(), equalTo(1));
        assertThat(serviceIdentification.getServiceTypeVersion().get(0), equalTo("1.0.0"));
    }

    @Test
    public void canGetLanguages() throws Exception {
        Languages languages = getCapabilitiesResponse.getLanguages();

        assertThat(languages.getDefault().getLanguage(), equalTo("EN"));
        assertThat(languages.getSupported().getLanguage().size(), equalTo(1));
        assertThat(languages.getSupported().getLanguage().get(0), equalTo("EN"));
    }

}