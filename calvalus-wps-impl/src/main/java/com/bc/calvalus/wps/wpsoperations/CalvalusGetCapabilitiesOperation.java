package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_ADDRESS;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_ADMINISTRATIVE_AREA;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_CITY;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_CONTACT_INSTRUCTION;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_COUNTRY;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_EMAIL_ADDRESS;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_FAX_NUMBER;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_NAME;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_PHONE_NUMBER;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_POST_CODE;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_SERVICE_HOURS;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.COMPANY_WEBSITE;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.PROJECT_MANAGER_NAME;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.PROJECT_MANAGER_POSITION_NAME;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_DEFAULT_LANG;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_GET_REQUEST_URL;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_POST_REQUEST_URL;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_SERVICE_ABSTRACT;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_SERVICE_ID;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_SERVICE_TYPE;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_SUPPORTED_LANG;
import static com.bc.calvalus.wps.wpsoperations.WpsConstants.WPS_VERSION;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.exceptions.ProcessesNotAvailableException;
import com.bc.calvalus.wps.responses.IWpsProcess;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.AddressType;
import com.bc.wps.api.schema.Capabilities;
import com.bc.wps.api.schema.CodeType;
import com.bc.wps.api.schema.ContactType;
import com.bc.wps.api.schema.DCP;
import com.bc.wps.api.schema.HTTP;
import com.bc.wps.api.schema.LanguageStringType;
import com.bc.wps.api.schema.Languages;
import com.bc.wps.api.schema.LanguagesType;
import com.bc.wps.api.schema.OnlineResourceType;
import com.bc.wps.api.schema.Operation;
import com.bc.wps.api.schema.OperationsMetadata;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.schema.ProcessOfferings;
import com.bc.wps.api.schema.RequestMethodType;
import com.bc.wps.api.schema.ResponsiblePartySubsetType;
import com.bc.wps.api.schema.ServiceIdentification;
import com.bc.wps.api.schema.ServiceProvider;
import com.bc.wps.api.schema.TelephoneType;
import com.bc.wps.api.utils.CapabilitiesBuilder;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusGetCapabilitiesOperation {

    private WpsRequestContext context;

    public CalvalusGetCapabilitiesOperation(WpsRequestContext context) {
        this.context = context;
    }

    public Capabilities getCapabilities() throws ProcessesNotAvailableException, JAXBException {
        List<IWpsProcess> processes = getProcesses();

        return CapabilitiesBuilder.create()
                    .withOperationsMetadata(getOperationsMetadata())
                    .withServiceIdentification(getServiceIdentification())
                    .withServiceProvider(getServiceProvider())
                    .withProcessOfferings(getProcessOfferings(processes))
                    .withLanguages(getLanguages())
                    .build();
    }

    protected OperationsMetadata getOperationsMetadata() {
        OperationsMetadata operationsMetadata = new OperationsMetadata();

        Operation getCapabilitiesOperation = new Operation();
        getCapabilitiesOperation.setName("GetCapabilities");
        DCP getCapabilitiesDcp = getGetDcp(WPS_GET_REQUEST_URL);
        getCapabilitiesOperation.getDCP().add(getCapabilitiesDcp);

        Operation describeProcessOperation = new Operation();
        describeProcessOperation.setName("DescribeProcess");
        DCP describeProcessDcp = getGetDcp(WPS_GET_REQUEST_URL);
        describeProcessOperation.getDCP().add(describeProcessDcp);

        Operation executeOperation = new Operation();
        executeOperation.setName("Execute");
        DCP executeDcp = getPostDcp(WPS_POST_REQUEST_URL);
        executeOperation.getDCP().add(executeDcp);

        Operation getStatusOperation = new Operation();
        getStatusOperation.setName("GetStatus");
        DCP getStatusDcp = getGetDcp(WPS_GET_REQUEST_URL);
        getStatusOperation.getDCP().add(getStatusDcp);

        operationsMetadata.getOperation().add(getCapabilitiesOperation);
        operationsMetadata.getOperation().add(describeProcessOperation);
        operationsMetadata.getOperation().add(executeOperation);
        operationsMetadata.getOperation().add(getStatusOperation);

        return operationsMetadata;
    }

    private DCP getPostDcp(String serviceUrl) {
        DCP executeDcp = new DCP();
        HTTP executeHttp = new HTTP();
        RequestMethodType executeRequestMethod = new RequestMethodType();
        executeRequestMethod.setHref(serviceUrl);
        executeHttp.setPost(executeRequestMethod);
        executeDcp.setHTTP(executeHttp);
        return executeDcp;
    }

    private DCP getGetDcp(String serviceUrl) {
        DCP describeProcessDcp = new DCP();
        HTTP describeProcessHttp = new HTTP();
        RequestMethodType describeProcessRequestMethod = new RequestMethodType();
        describeProcessRequestMethod.setHref(serviceUrl);
        describeProcessHttp.setGet(describeProcessRequestMethod);
        describeProcessDcp.setHTTP(describeProcessHttp);
        return describeProcessDcp;
    }

    protected ServiceProvider getServiceProvider() {
        ServiceProvider serviceProvider = new ServiceProvider();
        serviceProvider.setProviderName(COMPANY_NAME);

        OnlineResourceType siteUrl = new OnlineResourceType();
        siteUrl.setHref(COMPANY_WEBSITE);
        serviceProvider.setProviderSite(siteUrl);

        ResponsiblePartySubsetType contact = new ResponsiblePartySubsetType();
        contact.setIndividualName(PROJECT_MANAGER_NAME);
        contact.setPositionName(PROJECT_MANAGER_POSITION_NAME);

        ContactType contactInfo = new ContactType();

        TelephoneType phones = new TelephoneType();
        phones.getVoice().add(COMPANY_PHONE_NUMBER);
        phones.getFacsimile().add(COMPANY_FAX_NUMBER);
        contactInfo.setPhone(phones);

        AddressType address = new AddressType();
        address.getDeliveryPoint().add(COMPANY_ADDRESS);
        address.setCity(COMPANY_CITY);
        address.setAdministrativeArea(COMPANY_ADMINISTRATIVE_AREA);
        address.setPostalCode(COMPANY_POST_CODE);
        address.setCountry(COMPANY_COUNTRY);
        address.getElectronicMailAddress().add(COMPANY_EMAIL_ADDRESS);
        contactInfo.setAddress(address);

        contactInfo.setOnlineResource(siteUrl);
        contactInfo.setHoursOfService(COMPANY_SERVICE_HOURS);
        contactInfo.setContactInstructions(COMPANY_CONTACT_INSTRUCTION);

        contact.setContactInfo(contactInfo);

        CodeType role = new CodeType();
        role.setValue("PointOfContact");
        contact.setRole(role);
        serviceProvider.setServiceContact(contact);

        return serviceProvider;
    }

    protected ProcessOfferings getProcessOfferings(List<IWpsProcess> processList) {
        ProcessOfferings processOfferings = new ProcessOfferings();
        for (IWpsProcess process : processList) {
            ProcessBriefType singleProcessor = new ProcessBriefType();

            CodeType identifier = new CodeType();
            identifier.setValue(process.getIdentifier());
            singleProcessor.setIdentifier(identifier);

            LanguageStringType title = new LanguageStringType();
            title.setValue(process.getTitle());
            singleProcessor.setTitle(title);

            LanguageStringType abstractText = new LanguageStringType();
            abstractText.setValue(process.getAbstractText());
            singleProcessor.setAbstract(abstractText);

            processOfferings.getProcess().add(singleProcessor);
        }
        return processOfferings;
    }

    protected ServiceIdentification getServiceIdentification() {
        ServiceIdentification serviceIdentification = new ServiceIdentification();
        LanguageStringType title = new LanguageStringType();
        title.setValue(WPS_SERVICE_ID);
        serviceIdentification.setTitle(title);

        LanguageStringType abstractText = new LanguageStringType();
        abstractText.setValue(WPS_SERVICE_ABSTRACT);
        serviceIdentification.setAbstract(abstractText);

        CodeType serviceType = new CodeType();
        serviceType.setValue(WPS_SERVICE_TYPE);
        serviceIdentification.setServiceType(serviceType);

        serviceIdentification.getServiceTypeVersion().add(0, WPS_VERSION);
        return serviceIdentification;
    }

    protected Languages getLanguages() {
        Languages languages = new Languages();

        Languages.Default defaultLanguage = new Languages.Default();
        defaultLanguage.setLanguage(WPS_DEFAULT_LANG);
        languages.setDefault(defaultLanguage);

        LanguagesType languageType = new LanguagesType();
        languageType.getLanguage().add(0, WPS_SUPPORTED_LANG);
        languages.setSupported(languageType);

        return languages;
    }

    private List<IWpsProcess> getProcesses() throws ProcessesNotAvailableException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(context);
            return calvalusFacade.getProcessors();
        } catch (IOException | ProductionException exception) {
            throw new ProcessesNotAvailableException("Unable to retrieve available processors", exception);
        }
    }
}
