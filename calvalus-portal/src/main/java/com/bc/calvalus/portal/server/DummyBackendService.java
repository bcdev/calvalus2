package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalParameter;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.PortalProductionStatus;

import javax.servlet.ServletContext;
import java.io.File;
import java.io.FileOutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An BackendService implementation that is useful for developing the portal.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class DummyBackendService implements BackendService {

    private final ServletContext servletContext;
    private final List<DummyProduction> productionList;
    private long counter;

    public DummyBackendService(ServletContext servletContext) {
        this.servletContext = servletContext;
        productionList = Collections.synchronizedList(new ArrayList<DummyProduction>(32));
        // Add some dummy productions
        productionList.add(new DummyProduction("Formatting all hard drives", 20 * 1000));
        productionList.add(new DummyProduction("Drying CD slots", 10 * 1000));
        productionList.add(new DummyProduction("Rewriting kernel using BASIC", 5 * 1000));
        for (DummyProduction production : productionList) {
            production.start();
        }
    }

    @Override
    public PortalProductSet[] getProductSets(String filter) throws BackendServiceException {
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
    public PortalProcessor[] getProcessors(String filter) throws BackendServiceException {
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
    public PortalProduction[] getProductions(String filter) throws BackendServiceException {
        PortalProduction[] result = new PortalProduction[productionList.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = createPortalProduction(productionList.get(i));

        }
        return result;
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

        long secondsToRun = (int) (10 + 20 * Math.random()); // 10...30 seconds
        DummyProduction production = new DummyProduction(productionName, secondsToRun * 1000);
        production.start();

        productionList.add(production);

        return new PortalProductionResponse(createPortalProduction(production));
    }

    @Override
    public boolean[] cancelProductions(String[] productionIds) throws BackendServiceException {
        boolean[] results = new boolean[productionIds.length];
        for (int i = 0; i < productionIds.length; i++) {
            DummyProduction production = getProduction(productionIds[i]);
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
            DummyProduction production = getProduction(productionIds[i]);
            if (production != null) {
                production.cancel();
                productionList.remove(production);
                results[i] = true;
            }
        }
        return results;
    }

    @Override
    public String stageProductionOutput(String productionId) throws BackendServiceException {
        try {
            return getOutputFile(productionId);
        } catch (Exception e) {
            throw new BackendServiceException("Staging failed: " + e.getMessage(), e);
        }
    }

    private PortalProductionStatus getProductionStatus(String productionId) throws BackendServiceException {
        DummyProduction production = getProduction(productionId);
        if (production == null) {
            throw new BackendServiceException("Unknown production ID: " + productionId);
        }
        PortalProductionStatus.State state;
        if (production.isCancelled()) {
            state = PortalProductionStatus.State.CANCELLED;
        } else if (production.isDone()) {
            state = PortalProductionStatus.State.COMPLETED;
        } else {
            state = PortalProductionStatus.State.IN_PROGRESS;
        }
        return new PortalProductionStatus(state, production.getProgress());
    }

    private PortalProduction createPortalProduction(DummyProduction production) throws BackendServiceException {
        return new PortalProduction(production.getId(),
                                         production.getName(),
                                         getProductionStatus(production.getId()));
    }

    private DummyProduction getProduction(String productionId) {
        for (DummyProduction production : productionList) {
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

    private String getOutputFile(String productionId) throws Exception {
        File localDownloadDir = new PortalConfig(servletContext).getLocalDownloadDir();
        String fileName = "test-" + productionId + ".dat";
        File file = new File(localDownloadDir, fileName);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            FileOutputStream stream = new FileOutputStream(file);
            byte[] buffer = new byte[1024 * 1024];
            try {
                for (int i = 0; i < 32; i++) {
                    Arrays.fill(buffer, (byte) i);
                    stream.write(buffer);
                }
            } finally {
                stream.close();
            }
        }
        return fileName;
    }

}
