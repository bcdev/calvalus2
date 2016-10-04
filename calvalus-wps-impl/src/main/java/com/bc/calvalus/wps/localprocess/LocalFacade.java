package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.ceres.binding.BindingException;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.Execute;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * @author hans
 */
public class LocalFacade extends ProcessFacade {

    private final ProcessorExtractor processorExtractor;
    private final LocalStaging localStaging;
    private final String hostName;
    private final int portNumber;

    public LocalFacade(WpsRequestContext wpsRequestContext) throws IOException {
        super(wpsRequestContext);
        WpsServerContext serverContext = wpsRequestContext.getServerContext();
        this.hostName = serverContext.getHostAddress();
        this.portNumber = serverContext.getPort();
        this.processorExtractor = new ProcessorExtractor();
        this.localStaging = new LocalStaging();
    }

    @Override
    public LocalProductionStatus orderProductionAsynchronous(Execute executeRequest) throws WpsProductionException {
        return null;
    }

    @Override
    public LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException {
        return null;
    }

    @Override
    public List<String> getProductResultUrls(String jobId) throws WpsResultProductException {
        return localStaging.getProductUrls(jobId, userName, hostName, portNumber);
    }

    @Override
    public void stageProduction(String jobId) throws WpsStagingException {

    }

    @Override
    public void observeStagingStatus(String jobId) throws WpsStagingException {

    }

    @Override
    public List<WpsProcess> getProcessors() throws WpsProcessorNotFoundException {
        try {
            return processorExtractor.getProcessors();
        } catch (BindingException | IOException | URISyntaxException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }

    @Override
    public WpsProcess getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException {
        try {
            return processorExtractor.getProcessor(parser);
        } catch (BindingException | IOException | URISyntaxException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }
}
