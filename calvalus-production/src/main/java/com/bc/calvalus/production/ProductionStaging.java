package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.staging.Staging;

import java.util.Observable;
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
    private Observable productionService = null;

    public ProductionStaging(Production production) {
        this.production = production;
    }

    public Production getProduction() {
        return production;
    }

    public void setProductionService(Observable productionService) {
        this.productionService = productionService;
    }

    @Override
    public final void run() {
        try {
            performStaging();
            notifyRequestObservers();
        } catch (Throwable t) {
            production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, 1.0F, t.getMessage()));
            final String msg = String.format("Staging of production '%s' failed: %s", production.getId(), t.getMessage());
            Logger.getLogger("com.bc.calvalus").log(Level.SEVERE, msg, t);
        }
    }

    public static String getSafeFilename(String filename) {
        return filename.replace("/", "").replace("\\", "").replace(" ", "_");
    }

    public void notifyRequestObservers() {
        if (productionService != null) {
            productionService.notifyObservers(production);
        }
    }

}
