package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.calvalusfacade.IWpsProcess;

/**
 * @author hans
 */
public class LocalSubsetProcessor implements IWpsProcess {

    @Override
    public String getIdentifier() {
        return "local~0.0.1~Subset";
    }

    @Override
    public String getTitle() {
        return "A local subsetting service for Urban TEP";
    }

    @Override
    public String getAbstractText() {
        return "A local subsetting service for Urban TEP";
    }

    @Override
    public String getVersion() {
        return "0.0.1";
    }
}
