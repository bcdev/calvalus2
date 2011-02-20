package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

// todo - use ProductionService interface from calvalus-production

/**
 * The server side implementation of the RPC processing service.
 */
@SuppressWarnings("serial")
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {
    // for testing only...
    Map<String, Production> productions = new HashMap<String, Production>();

    @Override
    public PortalProductSet[] getProductSets(String type) throws BackendServiceException {
        return new PortalProductSet[]{
                new PortalProductSet("ps1", "MERIS-L1B", "MERIS RR 2004-2009"),
                new PortalProductSet("ps2", "MERIS-L1B", "MERIS RR 2004"),
                new PortalProductSet("ps3", "MERIS-L1B", "MERIS RR 2005"),
                new PortalProductSet("ps4", "MERIS-L1B", "MERIS RR 2006"),
                new PortalProductSet("ps5", "MERIS-L1B", "MERIS RR 2007"),
                new PortalProductSet("ps6", "MERIS-L1B", "MERIS RR 2008"),
                new PortalProductSet("ps7", "MERIS-L1B", "MERIS RR 2009"),
        };
    }

    @Override
    public PortalProcessor[] getProcessors(String type) throws BackendServiceException {
        return new PortalProcessor[]{
                new PortalProcessor("pc1", "MERIS-L2", "MERIS IOP Case2R",
                                    new String[]{"1.5-SNAPSHOT", "1.4", "1.3", "1.3-marco3"}),
                new PortalProcessor("pc2", "MERIS-L2", "MERIS IOP QAA",
                                    new String[]{"1.2-SNAPSHOT", "1.1.3", "1.0.1"}),
        };
    }


    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {

        if (!PortalProductionRequest.isValid(productionRequest)) {
            throw new BackendServiceException("Invalid processing request.");
        }

        String productionId = Long.toHexString(System.nanoTime());
        String productionName = MessageFormat.format("Processing {0} to {1} using {2}, version {3}.",
                                                     productionRequest.getInputProductSetId(),
                                                     productionRequest.getOutputProductSetName(),
                                                     productionRequest.getProcessorId(),
                                                     productionRequest.getProcessorVersion());

        long totalTime = 10 * 1000; // 10 seconds
        Production production = new Production(totalTime);
        production.start();

        productions.put(productionId, production);

        return new PortalProductionResponse(productionId, productionName, productionRequest);
    }

    @Override
    public WorkStatus getProductionStatus(String productionId) throws BackendServiceException {
        log("Someone wants update on production " + productionId);
        Production production = productions.get(productionId);
        if (production == null) {
            throw new BackendServiceException("Unknown production ID: " + productionId);
        }
        return new WorkStatus(production.isDone() ? WorkStatus.State.DONE : WorkStatus.State.IN_PROGRESS,
                              "Production " + production.getProgress() + "% completed",
                              production.getProgress());
    }

    private static class Production {
        private final long startTime;
        private final long totalTime;
        private Timer timer;
        private double progress;
        private boolean done;

        public Production(long totalTime) {
            this.totalTime = totalTime;
            this.startTime = System.currentTimeMillis();
        }

        public void start() {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    progress = (double) (System.currentTimeMillis() - startTime) / (double) totalTime;
                    if (progress >= 1.0) {
                        timer.cancel();
                        timer = null;
                        progress = 1.0;
                        done = true;
                    }
                }
            };
            timer = new Timer();
            timer.scheduleAtFixedRate(task, 0, 10);
        }

        public double getProgress() {
            return progress;
        }

        public boolean isDone() {
            return done;
        }
    }
}
