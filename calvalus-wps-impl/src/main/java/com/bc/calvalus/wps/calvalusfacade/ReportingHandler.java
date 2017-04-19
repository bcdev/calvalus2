package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionService;

import java.util.Map;
import java.util.Observable;
import java.util.Observer;

/**
 * TODO add API doc
 *
 * @author Martin
 * @author Muhammad
 */
public class ReportingHandler implements Observer {
    private String reportPath;
    public ReportingHandler(ProductionService productionService, String reportPath) {
        this.reportPath = reportPath;
        CalvalusLogger.getLogger().info("reporting handler created to log into " + reportPath);
    }

    @Override
    public void update(Observable o, Object arg) {
        Production production = (Production) arg;
        CalvalusLogger.getLogger().info("request " + production.getName() + " reported " + production.getProcessingStatus() + " should be logged in " + reportPath);
    }
}
