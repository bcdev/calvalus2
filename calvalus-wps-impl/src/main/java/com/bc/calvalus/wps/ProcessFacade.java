package com.bc.calvalus.wps;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.IWpsProcess;
import com.bc.calvalus.wps.cmd.LdapHelper;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public abstract class ProcessFacade {

    protected final String userName;

    public ProcessFacade(WpsRequestContext wpsRequestContext) throws IOException {
        this.userName = resolveUserName(wpsRequestContext);
    }

    public String getUserName() {
        return this.userName;
    }

    private String resolveUserName(WpsRequestContext wpsRequestContext) throws IOException {
        String remoteUserName = wpsRequestContext.getHeaderField("remote_user");
        if (remoteUserName != null) {
            remoteUserName = "tep_" + remoteUserName;
            LdapHelper ldap = new LdapHelper();
            if (!ldap.isRegistered(remoteUserName)) {
                ldap.register(remoteUserName);
            }
            return remoteUserName;
        } else {
            return wpsRequestContext.getUserName();
        }
    }

    public abstract String orderProductionAsynchronous(ProductionRequest request) throws WpsProductionException;

    public abstract String orderProductionSynchronous(ProductionRequest request) throws WpsProductionException;

    public abstract List<String> getProductResultUrls(String jobId) throws IOException, ProductionException;

    public abstract void stageProduction(String jobId) throws WpsStagingException;

    public abstract void observeStagingStatus(String jobId) throws WpsStagingException;

    public abstract List<IWpsProcess> getProcessors() throws WpsProcessorNotFoundException;

    public abstract IWpsProcess getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException;

}
