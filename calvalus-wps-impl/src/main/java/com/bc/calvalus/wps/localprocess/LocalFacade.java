package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.SqlStoreException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.Execute;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class LocalFacade extends ProcessFacade {

    private final LocalProcessorExtractor processorExtractor;
    private final LocalProduction localProduction;
    private final LocalStaging localStaging;
    private final String hostName;
    private final int portNumber;
    private final WpsRequestContext wpsRequestContext;

    public LocalFacade(WpsRequestContext wpsRequestContext) throws IOException {
        super(wpsRequestContext);
        WpsServerContext serverContext = wpsRequestContext.getServerContext();
        this.hostName = serverContext.getHostAddress();
        this.portNumber = serverContext.getPort();
        this.processorExtractor = new LocalProcessorExtractor();
        this.localStaging = new LocalStaging();
        this.localProduction = new LocalProduction();
        this.wpsRequestContext = wpsRequestContext;
    }

    @Override
    public LocalProductionStatus orderProductionAsynchronous(Execute executeRequest) throws WpsProductionException {
        return localProduction.orderProductionAsynchronous(executeRequest, systemUserName, remoteUserName, wpsRequestContext, this);
    }

    @Override
    public LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException {
        return localProduction.orderProductionSynchronous(executeRequest, systemUserName, remoteUserName, wpsRequestContext, this);
    }

    @Override
    public List<String> getProductResultUrls(String jobId) throws WpsResultProductException {
        return localStaging.getProductUrls(jobId, systemUserName, remoteUserName, hostName, portNumber);
    }

    @Override
    public void stageProduction(String jobId) throws WpsStagingException {

    }

    @Override
    public void observeStagingStatus(String jobId) throws WpsStagingException {

    }

    @Override
    public void generateProductMetadata(String jobId) throws ProductMetadataException {
        localStaging.generateProductMetadata(jobId, remoteUserName, hostName, portNumber);
    }

    @Override
    public List<WpsProcess> getProcessors() throws WpsProcessorNotFoundException {
        return processorExtractor.getProcessors(remoteUserName);
    }

    @Override
    public WpsProcess getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException {
        return processorExtractor.getProcessor(parser, remoteUserName);
    }

    private LocalProductionService getProductionService() throws SqlStoreException {
        return GpfProductionService.getProductionServiceSingleton();
    }
}
