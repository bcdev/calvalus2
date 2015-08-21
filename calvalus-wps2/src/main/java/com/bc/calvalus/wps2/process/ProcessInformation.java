package com.bc.calvalus.wps2.process;

import com.bc.calvalus.wps2.Processor;
import com.bc.calvalus.wps2.jaxb.CodeType;
import com.bc.calvalus.wps2.jaxb.LanguageStringType;
import com.bc.calvalus.wps2.jaxb.ProcessBriefType;

/**
 * Created by hans on 14/08/2015.
 */
public class ProcessInformation {

    private ProcessBriefType processData;

    public ProcessInformation(ProcessBriefType processData, Processor processor) {
        this.processData = processData;
        extractProcessorInformation(processor);
    }

    public ProcessBriefType getProcessData() {
        return processData;
    }

    private void extractProcessorInformation(Processor processor) {
        CodeType identifier = new CodeType();
        identifier.setValue(processor.getIdentifier());
        processData.setIdentifier(identifier);

        LanguageStringType title = new LanguageStringType();
        title.setValue(processor.getTitle());
        processData.setTitle(title);

        LanguageStringType abstractText = new LanguageStringType();
        abstractText.setValue(processor.getAbstractText());
        processData.setAbstract(abstractText);
    }
}
