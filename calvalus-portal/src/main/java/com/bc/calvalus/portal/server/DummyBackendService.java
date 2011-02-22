package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalParameter;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * An BackendService implementation that is useful for developing the portal.
 */
public class DummyBackendService implements BackendService {

    final List<Production> productionList;
    long counter;

    public DummyBackendService() {
        productionList = Collections.synchronizedList(new ArrayList<Production>(32));
        // Add some dummy productions
        productionList.add(new Production("Formatting all hard drives", 40 * 1000));
        productionList.add(new Production("Drying CD slots", 20 * 1000));
        productionList.add(new Production("Rewriting kernel using BASIC", 30 * 1000));
        for (Production production : productionList) {
            production.start();
        }
    }

    @Override
    public PortalProductSet[] getProductSets(String type) throws BackendServiceException {
        // Return some dummy product sets
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
        // Return some dummy processors
        return new PortalProcessor[]{
                new PortalProcessor("pc1", "MERIS-L2", "MERIS IOP Case2R",
                                    new String[]{"1.5-SNAPSHOT", "1.4", "1.3", "1.3-marco3"}),
                new PortalProcessor("pc2", "MERIS-L2", "MERIS IOP QAA",
                                    new String[]{"1.2-SNAPSHOT", "1.1.3", "1.0.1"}),
                new PortalProcessor("pc3", "General", "Band Maths",
                                    new String[]{"4.8"}),
        };
    }

    @Override
    public PortalProduction[] getProductions(String type) throws BackendServiceException {
        PortalProduction[] productions = new PortalProduction[productionList.size()];
        for (int i = 0; i < productions.length; i++) {
            productions[i] = new PortalProduction(productionList.get(i).getId(),
                                                  productionList.get(i).getName());

        }
        return productions;
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {

        if (!PortalProductionRequest.isValid(productionRequest)) {
            throw new BackendServiceException("Invalid processing request.");
        }

        String productionType = productionRequest.getProductionType();
        String outputFileName = getProductionParameter(productionRequest, "outputFileName")
                .replace("${user}", System.getProperty("user.name", "hadoop"))
                .replace("${type}", productionType)
                .replace("${num}", (++counter) + "");
        String inputProductSetId = getProductionParameter(productionRequest, "inputProductSetId");
        String productionName = MessageFormat.format("Producing file ''{0}'' from ''{1}'' using workflow ''{2}''",
                                                     outputFileName,
                                                     inputProductSetId,
                                                     productionType);

        long secondsToRun = (int) (10 + 50 * Math.random()); // 10...60 seconds
        Production production = new Production(productionName, secondsToRun * 1000);
        production.start();

        productionList.add(production);

        return new PortalProductionResponse(new PortalProduction(production.getId(),
                                                                 productionName),
                                            productionRequest);
    }

    @Override
    public WorkStatus getProductionStatus(String productionId) throws BackendServiceException {
        Production production = getProduction(productionId);
        if (production == null) {
            throw new BackendServiceException("Unknown production ID: " + productionId);
        }
        WorkStatus.State state;
        if (production.isCancelled()) {
            state = WorkStatus.State.CANCELLED;
        } else if (production.isDone()) {
            state = WorkStatus.State.COMPLETED;
        } else {
            state = WorkStatus.State.IN_PROGRESS;
        }
        return new WorkStatus(state,
                              production.getName(),
                              production.getProgress());
    }

    @Override
    public boolean[] cancelProductions(String[] productionIds) throws BackendServiceException {
        boolean[] results = new boolean[productionIds.length];
        for (int i = 0; i < productionIds.length; i++) {
            Production production = getProduction(productionIds[i]);
            if (production != null) {
                production.cancel();
                results[i] = true;
            }
        }
        return results;
    }

    @Override
    public boolean[] deleteProductions(String[] productionIds) throws BackendServiceException {
        boolean[] results = new boolean[productionIds.length];
        for (int i = 0; i < productionIds.length; i++) {
            Production production = getProduction(productionIds[i]);
            if (production != null) {
                production.cancel();
                productionList.remove(production);
                results[i] = true;
            }
        }
        return results;
    }

    private Production getProduction(String productionId) {
        for (Production production : productionList) {
            if (productionId.equals(production.getId())) {
                return production;
            }
        }
        return null;
    }


    public static String getProductionParameter(PortalProductionRequest productionRequest, String name) {
        for (PortalParameter parameter : productionRequest.getProductionParameters()) {
            if (name.equals(parameter.getName())) {
                return parameter.getValue();
            }
        }
        return null;
    }

    /**
     * Dummy production that simulates progress over a given amount of time.
     */
    static class Production {
        private static final Random idGen = new Random();
        private final String id;
        private final String name;
        private final long startTime;
        private final long totalTime;
        private Timer timer;
        private double progress;
        private boolean cancelled;

        /**
         * Constructs a new dummy production.
         *
         * @param name      Some name.
         * @param totalTime The total time in ms to run.
         */
        public Production(String name, long totalTime) {
            this.id = Long.toHexString(idGen.nextLong());
            this.name = name;
            this.totalTime = totalTime;
            this.startTime = System.currentTimeMillis();
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public double getProgress() {
            return progress;
        }

        public boolean isDone() {
            return timer == null;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void start() {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    progress = (double) (System.currentTimeMillis() - startTime) / (double) totalTime;
                    if (progress >= 1.0) {
                        progress = 1.0;
                        if (timer != null) {
                            stopTimer();
                        }
                    }
                }
            };
            timer = new Timer();
            timer.scheduleAtFixedRate(task, 0, 100);
            cancelled = false;
        }

        public void cancel() {
            if (timer != null) {
                stopTimer();
                cancelled = true;
            }
        }

        private void stopTimer() {
            timer.cancel();
            timer = null;
        }

    }
}
