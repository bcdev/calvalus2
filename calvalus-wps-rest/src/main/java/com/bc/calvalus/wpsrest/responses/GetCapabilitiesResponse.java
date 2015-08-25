package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.jaxb.AddressType;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.ContactType;
import com.bc.calvalus.wpsrest.jaxb.LanguageStringType;
import com.bc.calvalus.wpsrest.jaxb.Languages;
import com.bc.calvalus.wpsrest.jaxb.LanguagesType;
import com.bc.calvalus.wpsrest.jaxb.OnlineResourceType;
import com.bc.calvalus.wpsrest.jaxb.ProcessBriefType;
import com.bc.calvalus.wpsrest.jaxb.ProcessOfferings;
import com.bc.calvalus.wpsrest.jaxb.ResponsiblePartySubsetType;
import com.bc.calvalus.wpsrest.jaxb.ServiceIdentification;
import com.bc.calvalus.wpsrest.jaxb.ServiceProvider;
import com.bc.calvalus.wpsrest.jaxb.TelephoneType;

import java.util.List;

/**
 * Created by hans on 13/08/2015.
 */
public class GetCapabilitiesResponse {

    public Capabilities createGetCapabilitiesResponse(List<Processor> processors) {
        ServiceIdentification serviceIdentification = getServiceIdentification();
        ServiceProvider serviceProvider = getServiceProvider();
        ProcessOfferings processOfferings = getProcessOfferings(processors);
        Languages languages = getLanguages();

        return CapabilitiesBuilder.create()
                    .withServiceIdentification(serviceIdentification)
                    .withServiceProvider(serviceProvider)
                    .withProcessOfferings(processOfferings)
                    .withLanguages(languages)
                    .build();
    }

    private ServiceProvider getServiceProvider() {
        ServiceProvider serviceProvider = new ServiceProvider();
        serviceProvider.setProviderName("Brockmann-Consult");

        OnlineResourceType siteUrl = new OnlineResourceType();
        siteUrl.setHref("http://www.brockmann-consult.de");
        serviceProvider.setProviderSite(siteUrl);

        ResponsiblePartySubsetType contact = new ResponsiblePartySubsetType();
        contact.setIndividualName("Dr. Carsten Brockmann");
        contact.setPositionName("Project Manager");

        ContactType contactInfo = new ContactType();

        TelephoneType phones = new TelephoneType();
        phones.getVoice().add("+49 4152 889 301");
        phones.getFacsimile().add("+49 4152 889 333");
        contactInfo.setPhone(phones);

        AddressType address = new AddressType();
        address.getDeliveryPoint().add("Max-Planck-Str. 2");
        address.setCity("Geesthacht");
        address.setAdministrativeArea("SH");
        address.setPostalCode("21502");
        address.setCountry("Germany");
        address.getElectronicMailAddress().add("carsten.brockmann@brockmann-consult.de");
        contactInfo.setAddress(address);

        contactInfo.setOnlineResource(siteUrl);
        contactInfo.setHoursOfService("24x7");
        contactInfo.setContactInstructions("Don't hesitate to call");

        contact.setContactInfo(contactInfo);

        CodeType role = new CodeType();
        role.setValue("PointOfContact");
        contact.setRole(role);
        serviceProvider.setServiceContact(contact);

        return serviceProvider;
    }

    private ProcessOfferings getProcessOfferings(List<Processor> processors) {
        ProcessOfferings processOfferings = new ProcessOfferings();
        for (Processor processor : processors) {
            ProcessBriefType singleProcessor = new ProcessBriefType();

            CodeType identifier = new CodeType();
            identifier.setValue(processor.getIdentifier());
            singleProcessor.setIdentifier(identifier);

            LanguageStringType title = new LanguageStringType();
            title.setValue(processor.getTitle());
            singleProcessor.setTitle(title);

            LanguageStringType abstractText = new LanguageStringType();
            abstractText.setValue(processor.getAbstractText());
            singleProcessor.setAbstract(abstractText);

            processOfferings.getProcess().add(singleProcessor);
        }
        return processOfferings;
    }

    private ServiceIdentification getServiceIdentification() {
        ServiceIdentification serviceIdentification = new ServiceIdentification();
        LanguageStringType title = new LanguageStringType();
        title.setValue("Calvalus WPS server");
        serviceIdentification.setTitle(title);

        LanguageStringType abstractText = new LanguageStringType();
        abstractText.setValue("Web Processing Service for Calvalus");
        serviceIdentification.setAbstract(abstractText);

        CodeType serviceType = new CodeType();
        serviceType.setValue("WPS");
        serviceIdentification.setServiceType(serviceType);

        serviceIdentification.getServiceTypeVersion().add(0, "1.0.0");
        return serviceIdentification;
    }

    private Languages getLanguages() {
        Languages languages = new Languages();

        Languages.Default defaultLanguage = new Languages.Default();
        defaultLanguage.setLanguage("EN");
        languages.setDefault(defaultLanguage);

        LanguagesType languageType = new LanguagesType();
        languageType.getLanguage().add(0, "EN");
        languages.setSupported(languageType);

        return languages;
    }

}
