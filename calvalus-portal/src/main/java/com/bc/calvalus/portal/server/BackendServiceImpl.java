package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// todo - use ProductionService interface from calvalus-production

/**
 * The server side implementation of the RPC processing service.
 */
@SuppressWarnings("serial")
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    final List<Production> productionList;

    public BackendServiceImpl() {
        super();
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

        String productionName = MessageFormat.format("Processing {0} to {1} using {2}, version {3}.",
                                                     productionRequest.getInputProductSetId(),
                                                     productionRequest.getOutputProductSetName(),
                                                     productionRequest.getProcessorId(),
                                                     productionRequest.getProcessorVersion());

        long secondsToRun = (int) (10 + 30 * Math.random()); // 10...40 seconds
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
        return new WorkStatus(production.isDone() ? WorkStatus.State.DONE : WorkStatus.State.IN_PROGRESS,
                              production.getName(),
                              production.getProgress());
    }

    private Production getProduction(String productionId) {
        for (Production production : productionList) {
            if (productionId.equals(production.getId())) {
                return production;
            }
        }
        return null;
    }

}
