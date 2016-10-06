package com.bc.calvalus.wps;

import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.cmd.LdapHelper;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public abstract class ProcessFacade {

    protected final String userName;
    private static final String REMOTE_USER_KEY = PropertiesWrapper.get("remote.user.key");
    private static final String REMOTE_USER_PREFIX = PropertiesWrapper.get("remote.user.prefix");

    public ProcessFacade(WpsRequestContext wpsRequestContext) throws IOException {
        this.userName = resolveUserName(wpsRequestContext);
    }

    public String getUserName() {
        return this.userName;
    }

    private String resolveUserName(WpsRequestContext wpsRequestContext) throws IOException {
        String remoteUserName = wpsRequestContext.getHeaderField(REMOTE_USER_KEY);
        if (remoteUserName != null) {
            remoteUserName = REMOTE_USER_PREFIX + remoteUserName;
            LdapHelper ldap = new LdapHelper();
            if (!ldap.isRegistered(remoteUserName)) {
                ldap.register(remoteUserName);
            }
            return remoteUserName;
        } else {
            return wpsRequestContext.getUserName();
        }
    }

    public abstract LocalProductionStatus orderProductionAsynchronous(Execute executeRequest) throws WpsProductionException;

    public abstract LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException;

    public abstract List<String> getProductResultUrls(String jobId) throws WpsResultProductException;

    public abstract void stageProduction(String jobId) throws WpsStagingException;

    public abstract void observeStagingStatus(String jobId) throws WpsStagingException;

    public abstract void generateProductMetadata(String jobId) throws ProductMetadataException;

    public abstract List<WpsProcess> getProcessors() throws WpsProcessorNotFoundException;

    public abstract WpsProcess getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException;

}
