package com.bc.calvalus.wpsrest.responses;

import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_ADDRESS;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_ADMINISTRATIVE_AREA;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_CITY;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_CONTACT_INSTRUCTION;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_COUNTRY;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_EMAIL_ADDRESS;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_FAX_NUMBER;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_NAME;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_PHONE_NUMBER;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_POST_CODE;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_SERVICE_HOURS;
import static com.bc.calvalus.wpsrest.WpsConstants.COMPANY_WEBSITE;
import static com.bc.calvalus.wpsrest.WpsConstants.PROJECT_MANAGER_NAME;
import static com.bc.calvalus.wpsrest.WpsConstants.PROJECT_MANAGER_POSITION_NAME;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_DEFAULT_LANG;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_SERVICE_ABSTRACT;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_SERVICE_ID;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_SERVICE_TYPE;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_SUPPORTED_LANG;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_VERSION;

import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.jaxb.AddressType;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.ContactType;
import com.bc.calvalus.wpsrest.jaxb.LanguageStringType;
import com.bc.calvalus.wpsrest.jaxb.Languages;
import com.bc.calvalus.wpsrest.jaxb.LanguagesType;
import com.bc.calvalus.wpsrest.jaxb.OnlineResourceType;
import com.bc.calvalus.wpsrest.jaxb.Operation;
import com.bc.calvalus.wpsrest.jaxb.OperationsMetadata;
import com.bc.calvalus.wpsrest.jaxb.ProcessBriefType;
import com.bc.calvalus.wpsrest.jaxb.ProcessOfferings;
import com.bc.calvalus.wpsrest.jaxb.ResponsiblePartySubsetType;
import com.bc.calvalus.wpsrest.jaxb.ServiceIdentification;
import com.bc.calvalus.wpsrest.jaxb.ServiceProvider;
import com.bc.calvalus.wpsrest.jaxb.TelephoneType;

import java.util.List;

/**
 * @author hans
 */
public class CalvalusGetCapabilitiesResponse extends AbstractGetCapabilitiesResponse {

    public OperationsMetadata getOperationsMetadata() {
        OperationsMetadata operationsMetadata = new OperationsMetadata();

        Operation getCapabilitiesOperation = new Operation();
        getCapabilitiesOperation.setName("GetCapabilities");

        Operation describeProcessOperation = new Operation();
        describeProcessOperation.setName("DescribeProcess");

        Operation executeOperation = new Operation();
        executeOperation.setName("Execute");

        Operation getStatusOperation = new Operation();
        getStatusOperation.setName("GetStatus");

        operationsMetadata.getOperation().add(getCapabilitiesOperation);
        operationsMetadata.getOperation().add(describeProcessOperation);
        operationsMetadata.getOperation().add(executeOperation);
        operationsMetadata.getOperation().add(getStatusOperation);

        return operationsMetadata;
    }

    public ServiceProvider getServiceProvider() {
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

    public ProcessOfferings getProcessOfferings(List<WpsProcess> processList) {
        ProcessOfferings processOfferings = new ProcessOfferings();
        for (WpsProcess process : processList) {
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

    public ServiceIdentification getServiceIdentification() {
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

    public Languages getLanguages() {
        Languages languages = new Languages();

        Languages.Default defaultLanguage = new Languages.Default();
        defaultLanguage.setLanguage(WPS_DEFAULT_LANG);
        languages.setDefault(defaultLanguage);

        LanguagesType languageType = new LanguagesType();
        languageType.getLanguage().add(0, WPS_SUPPORTED_LANG);
        languages.setSupported(languageType);

        return languages;
    }

}
