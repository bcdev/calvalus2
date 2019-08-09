package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.localprocess.LocalFacade;
import com.bc.wps.api.WpsRequestContext;

import java.io.IOException;

/**
 * @author hans
 */
public class WpsOperation {

    protected final CalvalusFacade calvalusFacade;
    protected final LocalFacade localFacade;

    public WpsOperation(WpsRequestContext context) throws IOException {
        this.calvalusFacade = new CalvalusFacade(context);
        this.localFacade = new LocalFacade(context);
    }
}
