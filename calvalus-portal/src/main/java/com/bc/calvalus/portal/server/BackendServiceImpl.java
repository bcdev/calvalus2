package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import java.text.MessageFormat;

/**
 * The server side implementation of the RPC processing service.
 */
@SuppressWarnings("serial")
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    @Override
    public PortalProductSet[] getProductSets(String type) throws BackendServiceException {
        return new PortalProductSet[] {
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
        return new PortalProcessor[] {
                new PortalProcessor("pc1", "MERIS-L2", "MERIS IOP Case2R",
                                    new String[] {"1.5-SNAPSHOT", "1.4", "1.3", "1.3-marco3"}),
                new PortalProcessor("pc2", "MERIS-L2", "MERIS IOP QAA",
                                    new String[] {"1.2-SNAPSHOT", "1.1.3", "1.0.1"}),
        };
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest request) throws BackendServiceException {

        if (!PortalProductionRequest.isValid(request)) {
            throw new BackendServiceException("Invalid processing request.");
        }

        String message = MessageFormat.format("About to process {0} to {1} using {2}.",
                                             request.getInputProductSetId(),
                                             request.getOutputProductSetName(),
                                             request.getProcessorId());
        return new PortalProductionResponse(message);
    }


}
