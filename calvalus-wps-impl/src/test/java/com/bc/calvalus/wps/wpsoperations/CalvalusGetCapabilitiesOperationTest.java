package com.bc.calvalus.wps.wpsoperations;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.IWpsProcess;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.Capabilities;
import com.bc.wps.api.schema.Languages;
import com.bc.wps.api.schema.OperationsMetadata;
import com.bc.wps.api.schema.ProcessOfferings;
import com.bc.wps.api.schema.ServiceIdentification;
import com.bc.wps.api.schema.ServiceProvider;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusGetCapabilitiesOperation.class, CalvalusFacade.class,
            ProcessFacade.class, FileInputStream.class
})
public class CalvalusGetCapabilitiesOperationTest {

    private CalvalusGetCapabilitiesOperation getCapabilitiesOperation;
    private CalvalusFacade mockCalvalusFacade;
    private WpsRequestContext mockRequestContext;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        mockRequestContext = mock(WpsRequestContext.class);
        mockCalvalusFacade = mock(CalvalusFacade.class);

        getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(mockRequestContext);
    }

    @Ignore
    @Test
    public void canGetCapabilities() throws Exception {
        configureMockProcesses();

        Capabilities capabilities = getCapabilitiesOperation.getCapabilities();

        assertThat(capabilities.getOperationsMetadata().getOperation().size(), equalTo(4));
        assertThat(capabilities.getServiceProvider().getProviderName(), equalTo("Brockmann Consult GmbH"));
        assertThat(capabilities.getProcessOfferings().getProcess().size(), equalTo(3)); // always +1 at the moment due to local process
        assertThat(capabilities.getServiceIdentification().getTitle().getValue(), equalTo("Calvalus WPS server"));
        assertThat(capabilities.getLanguages().getDefault().getLanguage(), equalTo("EN"));

    }

    @Test
    public void canGetOperationsMetadata() throws Exception {
        OperationsMetadata operationsMetadata = getCapabilitiesOperation.getOperationsMetadata();

        assertThat(operationsMetadata.getOperation().size(), equalTo(4));
        assertThat(operationsMetadata.getOperation().get(0).getName(), equalTo("GetCapabilities"));
        assertThat(operationsMetadata.getOperation().get(0).getDCP().size(), equalTo(1));
        assertThat(operationsMetadata.getOperation().get(0).getDCP().get(0).getHTTP().getGet().getHref(),
                   equalTo("http://www.brockmann-consult.de/bc-wps/wps/calvalus?"));

        assertThat(operationsMetadata.getOperation().get(1).getName(), equalTo("DescribeProcess"));
        assertThat(operationsMetadata.getOperation().get(1).getDCP().size(), equalTo(1));
        assertThat(operationsMetadata.getOperation().get(1).getDCP().get(0).getHTTP().getGet().getHref(),
                   equalTo("http://www.brockmann-consult.de/bc-wps/wps/calvalus?"));

        assertThat(operationsMetadata.getOperation().get(2).getName(), equalTo("Execute"));
        assertThat(operationsMetadata.getOperation().get(2).getDCP().size(), equalTo(1));
        assertThat(operationsMetadata.getOperation().get(2).getDCP().get(0).getHTTP().getPost().getHref(),
                   equalTo("http://www.brockmann-consult.de/bc-wps/wps/calvalus"));

        assertThat(operationsMetadata.getOperation().get(3).getName(), equalTo("GetStatus"));
        assertThat(operationsMetadata.getOperation().get(3).getDCP().size(), equalTo(1));
        assertThat(operationsMetadata.getOperation().get(3).getDCP().get(0).getHTTP().getGet().getHref(),
                   equalTo("http://www.brockmann-consult.de/bc-wps/wps/calvalus?"));
    }

    @Test
    public void canGetServiceProvider() throws Exception {
        ServiceProvider serviceProvider = getCapabilitiesOperation.getServiceProvider();

        assertThat(serviceProvider.getProviderName(), equalTo("Brockmann Consult GmbH"));
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
        configureMockProcesses();
        getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(mockRequestContext);
        ProcessOfferings processOfferings = getCapabilitiesOperation.getProcessOfferings();

        assertThat(processOfferings.getProcess().size(), equalTo(3)); // always +1 at the moment due to local process
        assertThat(processOfferings.getProcess().get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processOfferings.getProcess().get(0).getTitle().getValue(), equalTo("Band arythmetic processor"));
        assertThat(processOfferings.getProcess().get(0).getAbstract().getValue(), equalTo("Some description"));

        assertThat(processOfferings.getProcess().get(1).getIdentifier().getValue(), equalTo("beam-buildin~1.0~urban-tep-indices"));
        assertThat(processOfferings.getProcess().get(1).getTitle().getValue(), equalTo("Urban TEP seasonality indices from MERIS SR"));
        assertThat(processOfferings.getProcess().get(1).getAbstract().getValue(), equalTo("Some description"));

        assertThat(processOfferings.getProcess().get(2).getIdentifier().getValue(), equalTo("urbantep-local~1.0~Subset"));
        assertThat(processOfferings.getProcess().get(2).getTitle().getValue(), equalTo("Urban TEP local subsetting"));
        assertThat(processOfferings.getProcess().get(2).getAbstract().getValue(), equalTo("Urban TEP local subsetting"));

    }

    @Test
    public void canGetServiceIdentification() throws Exception {
        ServiceIdentification serviceIdentification = getCapabilitiesOperation.getServiceIdentification();

        assertThat(serviceIdentification.getTitle().getValue(), equalTo("Calvalus WPS server"));
        assertThat(serviceIdentification.getAbstract().getValue(), equalTo("Web Processing Service for Calvalus"));
        assertThat(serviceIdentification.getServiceType().getValue(), equalTo("WPS"));
        assertThat(serviceIdentification.getServiceTypeVersion().size(), equalTo(1));
        assertThat(serviceIdentification.getServiceTypeVersion().get(0), equalTo("1.0.0"));
    }

    @Test
    public void canGetLanguages() throws Exception {
        Languages languages = getCapabilitiesOperation.getLanguages();

        assertThat(languages.getDefault().getLanguage(), equalTo("EN"));
        assertThat(languages.getSupported().getLanguage().size(), equalTo(1));
        assertThat(languages.getSupported().getLanguage().get(0), equalTo("EN"));
    }

    private void configureMockProcesses() throws Exception {

        List<IWpsProcess> mockProcessList = getMockWpsProcesses();
        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessList);
        PowerMockito.whenNew(CalvalusFacade.class).withAnyArguments().thenReturn(mockCalvalusFacade);
    }

    private List<IWpsProcess> getMockWpsProcesses() {
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
        return mockProcessList;
    }

}