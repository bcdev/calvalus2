package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.staging.Staging;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A production staging.
 *
 * @author MarcoP
 * @author Norman
 */
public abstract class ProductionStaging extends Staging {

    private final Production production;

    public ProductionStaging(Production production) {
        this.production = production;
    }

    public Production getProduction() {
        return production;
    }

    @Override
    public final void run() {
        try {
            performStaging();
        } catch (Throwable t) {
            production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, 1.0F, t.getMessage()));
            Logger.getLogger("com.bc.calvalus").log(Level.SEVERE,
                                                    String.format("Staging of production '%s' failed: %s",
                                                                  production.getId(), t.getMessage()),
                                                    t);
        }
    }
}
