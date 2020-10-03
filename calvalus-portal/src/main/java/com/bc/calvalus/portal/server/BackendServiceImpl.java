/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.server;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.DtoAggregatorDescriptor;
import com.bc.calvalus.portal.shared.DtoCalvalusConfig;
import com.bc.calvalus.portal.shared.DtoColorPalette;
import com.bc.calvalus.portal.shared.DtoMaskDescriptor;
import com.bc.calvalus.portal.shared.DtoParameterDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessState;
import com.bc.calvalus.portal.shared.DtoProcessStatus;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.bc.calvalus.portal.shared.DtoProductionResponse;
import com.bc.calvalus.portal.shared.DtoRegion;
import com.bc.calvalus.portal.shared.DtoRegionDataInfo;
import com.bc.calvalus.portal.shared.DtoValueRange;
import com.bc.calvalus.processing.AggregatorDescriptor;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.calvalus.processing.ma.RecordSourceSpi;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.ServiceContainerFactory;
import com.bc.calvalus.production.cli.WpsProductionRequestConverter;
import com.bc.calvalus.production.cli.WpsXmlRequestConverter;
import com.bc.calvalus.production.util.DateRangeCalculator;
import com.bc.calvalus.production.util.DebugTokenGenerator;
import com.bc.calvalus.production.util.TokenGenerator;
import com.bc.ceres.binding.ValueRange;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.esa.snap.core.datamodel.GeoPos;
import org.jasig.cas.client.validation.AssertionImpl;
import org.jdom.JDOMException;
import org.jdom2.Element;
import org.jdom2.input.DOMBuilder;
import org.jdom2.output.DOMOutputter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The server side implementation of the RPC processing service.
 * <p/>
 * The actual service object is created by a factory whose implementing class name is given by
 * the servlet initialisation parameter 'calvalus.portal.productionServiceFactory.class'
 * (in context.xml or web.xml).
 *
 * @author Norman
 * @author MarcoZ
 */
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final Properties calvalusVersionProperties;
    private static final String REQUEST_FILE_EXTENSION = ".xml";
    private static final String USER_ROLE = "calvalus.portal.userRole";

    static {
        InputStream in = BackendServiceImpl.class.getResourceAsStream("/calvalus-version.properties");
        calvalusVersionProperties = new Properties();
        try {
            calvalusVersionProperties.load(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String VERSION = String.format("Calvalus version %s (built %s)",
            calvalusVersionProperties.get("version"),
            calvalusVersionProperties.get("timestamp"));
    public static final String COPYRIGHT_YEAR = "2017";

    private static final int PRODUCTION_STATUS_OBSERVATION_PERIOD = 5000;

    private ServiceContainer serviceContainer;
    private BackendConfig backendConfig;
    private Timer statusObserver;
    private static final DateFormat CCSDS_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    /**
     * Overridden to do nothing. This is because it seems that Firefox 6 is not sending extra request header when set in the XmlHttpRequest object.
     * We then get on Tomcat 7 in the logs
     * <pre>
     *   16-Sep-2011 10:57:08 org.apache.catalina.core.ApplicationContext log
     *   SEVERE: Exception while dispatching incoming RPC call
     *   java.lang.SecurityException: Blocked request without GWT permutation header (XSRF attack?)
     *           at com.google.gwt.user.server.rpc.RemoteServiceServlet.checkPermutationStrongName(RemoteServiceServlet.java:272)
     *           at com.google.gwt.user.server.rpc.RemoteServiceServlet.processCall(RemoteServiceServlet.java:203)
     *           at com.google.gwt.user.server.rpc.RemoteServiceServlet.processPost(RemoteServiceServlet.java:248)
     *           at com.google.gwt.user.server.rpc.AbstractRemoteServiceServlet.doPost(AbstractRemoteServiceServlet.java:62)
     *           at javax.servlet.http.HttpServlet.service(HttpServlet.java:641)
     *           at javax.servlet.http.HttpServlet.service(HttpServlet.java:722)
     *           at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:304)
     *           at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:210)
     *           at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:240)
     *           at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:164)
     *   ...
     * </pre>
     * See http://code.google.com/p/gwteventservice/issues/detail?id=30 and<br/> http://jectbd.com/?p=1351  and<br/>
     * http://stackoverflow.com/questions/5429961/gwt-xsrf-sporadic-missing-x-gwt-permutation-header
     */
    @Override
    protected void checkPermutationStrongName() {
    }

    @Override
    public void init() throws ServletException {
        if (serviceContainer != null) {
            LOG.log(Level.WARNING, String.format("Found pre-existing service container %s, closing...", serviceContainer.toString()));
            statusObserver.cancel();
            try {
                serviceContainer.close();
            } catch (ProductionException e) {
                LOG.log(Level.WARNING, "Unable to close obsolete service container.", e);
            }
            serviceContainer = null;
            System.gc();
        }
        if (serviceContainer == null) {
            synchronized (this) {
                if (serviceContainer == null) {
                    ServletContext servletContext = getServletContext();
                    initLogger(servletContext);
                    initBackendConfig(servletContext);
                    initProductionService();
                    startObservingProductionService();
                }
            }
        }
    }

    @Override
    public void destroy() {
        if (serviceContainer != null) {
            statusObserver.cancel();
            try {
                serviceContainer.close();
            } catch (Exception e) {
                log("Failed to close production service", e);
            }
            serviceContainer = null;
        }
        super.destroy();
    }

    @Override
    public DtoRegion[] loadRegions(String filter) throws BackendServiceException {
        RegionPersistence regionPersistence = new RegionPersistence(getUserName(), ProductionServiceConfig.getUserAppDataDir());
        try {
            DtoRegion[] dtoRegions = regionPersistence.loadRegions();
            LOG.fine("loadRegions returns " + dtoRegions.length);
            return dtoRegions;
        } catch (IOException e) {
            log(e.getMessage(), e);
            throw new BackendServiceException("Failed to load regions: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeRegions(DtoRegion[] regions) throws BackendServiceException {
        RegionPersistence regionPersistence = new RegionPersistence(getUserName(), ProductionServiceConfig.getUserAppDataDir());
        try {
            regionPersistence.storeRegions(regions);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to store regions: " + e.getMessage(), e);
        }
    }

    @Override
    public DtoColorPalette[] loadColorPalettes(String filter) throws BackendServiceException {
        ColorPalettePersistence colorPalettePersistence = new ColorPalettePersistence(getUserName(), ProductionServiceConfig.getUserAppDataDir());
        try {
            DtoColorPalette[] dtoColorPalettes = colorPalettePersistence.loadColorPalettes();
            LOG.fine("loadColorPalettes returns " + dtoColorPalettes.length);
            return dtoColorPalettes;
        } catch (IOException e) {
            log(e.getMessage(), e);
            throw new BackendServiceException("Failed to load color palettes: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeColorPalettes(DtoColorPalette[] colorPalettes) throws BackendServiceException {
        ColorPalettePersistence colorPalettePersistence = new ColorPalettePersistence(getUserName(), ProductionServiceConfig.getUserAppDataDir());
        try {
            colorPalettePersistence.storeColorPalettes(colorPalettes);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to store color palettes: " + e.getMessage(), e);
        }
    }

    @Override
    public DtoProductSet[] getProductSets(String filter) throws BackendServiceException {
        if (filter.contains("dummy")) {
            filter = filter.replace("dummy", getUserName());
        }
        try {
            ProductSet[] productSets = serviceContainer.getInventoryService().getProductSets(getUserName(), filter);
            DtoProductSet[] dtoProductSets = new DtoProductSet[productSets.length];
            for (int i = 0; i < productSets.length; i++) {
                dtoProductSets[i] = convert(productSets[i]);
            }
            LOG.fine("getProductSets returns " + dtoProductSets.length);
            return dtoProductSets;
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProcessorDescriptor[] getProcessors(String filterString) throws BackendServiceException {
        try {
            List<DtoProcessorDescriptor> dtoProcessorDescriptors = new ArrayList<>();
            final BundleFilter filter = BundleFilter.fromString(filterString);
            String userName = getUserName();
            filter.withTheUser(userName);

            final BundleDescriptor[] bundleDescriptors = serviceContainer.getProductionService().getBundles(userName, filter);
            for (BundleDescriptor bundleDescriptor : bundleDescriptors) {
                DtoProcessorDescriptor[] dtoDescriptors = getDtoProcessorDescriptors(bundleDescriptor);
                dtoProcessorDescriptors.addAll(Arrays.asList(dtoDescriptors));
            }
            LOG.fine("getProcessors returns " + dtoProcessorDescriptors.size());
            return dtoProcessorDescriptors.toArray(new DtoProcessorDescriptor[dtoProcessorDescriptors.size()]);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoAggregatorDescriptor[] getAggregators(String filterString) throws BackendServiceException {
        try {
            List<DtoAggregatorDescriptor> dtoAggregatorDescriptors = new ArrayList<>();
            final BundleFilter filter = BundleFilter.fromString(filterString);
            String userName = getUserName();
            filter.withTheUser(userName);

            final BundleDescriptor[] bundleDescriptors = serviceContainer.getProductionService().getBundles(userName, filter);
            for (BundleDescriptor bundleDescriptor : bundleDescriptors) {
                DtoAggregatorDescriptor[] dtoDescriptors = getDtoAggregatorDescriptors(bundleDescriptor);
                dtoAggregatorDescriptors.addAll(Arrays.asList(dtoDescriptors));
            }
            LOG.fine("getAggregators returns " + dtoAggregatorDescriptors.size());
            return dtoAggregatorDescriptors.toArray(new DtoAggregatorDescriptor[dtoAggregatorDescriptors.size()]);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoMaskDescriptor[] getMasks() throws BackendServiceException {
        try {
            String userName = getUserName();
            final MaskDescriptor[] maskDescriptors = serviceContainer.getProductionService().getMasks(userName);
            return getDtoMaskDescriptors(maskDescriptors);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    private DtoMaskDescriptor[] getDtoMaskDescriptors(MaskDescriptor[] maskDescriptors) {
        List<DtoMaskDescriptor> dtoMaskDescriptors = new ArrayList<>(maskDescriptors.length);
        for (MaskDescriptor maskDescriptor : maskDescriptors) {
            dtoMaskDescriptors.add(convert(maskDescriptor));
        }
        return dtoMaskDescriptors.toArray(new DtoMaskDescriptor[0]);
    }

    private DtoMaskDescriptor convert(MaskDescriptor maskDescriptor) {
        return new DtoMaskDescriptor(maskDescriptor.getMaskName(), maskDescriptor.getMaskDescriptionHTML());
    }

    private DtoProcessorDescriptor[] getDtoProcessorDescriptors(BundleDescriptor bundleDescriptor) {
        ProcessorDescriptor[] processorDescriptors = bundleDescriptor.getProcessorDescriptors();
        if (processorDescriptors != null) {
            DtoProcessorDescriptor[] dtoDescriptors = new DtoProcessorDescriptor[processorDescriptors.length];
            for (int i = 0; i < processorDescriptors.length; i++) {
                dtoDescriptors[i] = convert(bundleDescriptor.getBundleName(),
                        bundleDescriptor.getBundleVersion(),
                        bundleDescriptor.getBundleLocation(),
                        bundleDescriptor.getOwner(),
                        processorDescriptors[i]);
            }
            return dtoDescriptors;
        } else {
            return new DtoProcessorDescriptor[]{
                    new DtoProcessorDescriptor(null,
                            BundleFilter.DUMMY_PROCESSOR_NAME,
                            "",
                            "",
                            bundleDescriptor.getBundleName(),
                            bundleDescriptor.getBundleVersion(),
                            bundleDescriptor.getBundleLocation(),
                            "",
                            null,
                            null,
                            DtoProcessorDescriptor.DtoProcessorCategory.LEVEL2,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null)
            };
        }
    }

    private DtoAggregatorDescriptor[] getDtoAggregatorDescriptors(BundleDescriptor bundleDescriptor) {
        AggregatorDescriptor[] aggregatorDescriptors = bundleDescriptor.getAggregatorDescriptors();
        if (aggregatorDescriptors != null) {
            DtoAggregatorDescriptor[] dtoDescriptors = new DtoAggregatorDescriptor[aggregatorDescriptors.length];
            for (int i = 0; i < aggregatorDescriptors.length; i++) {
                dtoDescriptors[i] = convert(bundleDescriptor.getBundleName(),
                        bundleDescriptor.getBundleVersion(),
                        bundleDescriptor.getBundleLocation(),
                        bundleDescriptor.getOwner(),
                        aggregatorDescriptors[i]);
            }
            return dtoDescriptors;
        } else {
            return new DtoAggregatorDescriptor[0];
        }
    }

    @Override
    public DtoProduction[] getProductions(String filter) throws BackendServiceException {
        boolean currentUserFilter = (PARAM_NAME_CURRENT_USER_ONLY + "=true").equals(filter);
        try {
            Production[] productions = serviceContainer.getProductionService().getProductions(filter);
            ArrayList<DtoProduction> dtoProductions = new ArrayList<>(productions.length);
            for (Production production : productions) {
                if (currentUserFilter) {
                    if (getUserName().equalsIgnoreCase(production.getProductionRequest().getUserName())) {
                        dtoProductions.add(convert(production));
                    }
                } else {
                    dtoProductions.add(convert(production));
                }
            }
            LOG.fine("getProductions returns " + dtoProductions.size());
            return dtoProductions.toArray(new DtoProduction[dtoProductions.size()]);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProductionRequest getProductionRequest(String productionId) throws BackendServiceException {
        try {
            Production production = serviceContainer.getProductionService().getProduction(productionId);
            if (production != null) {
                return convert(production.getId(), production.getProductionRequest());
            } else {
                return null;
            }
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProductionResponse orderProduction(DtoProductionRequest dtoProductionRequest) throws
            BackendServiceException {
        try {
            ProductionRequest productionRequest = convert(dtoProductionRequest);
            HadoopJobHook hook = null;
            Map<String, String> config = backendConfig.getConfigMap();
            String systemName = config.get("calvalus.system.name");
            if(StringUtils.isNotBlank(systemName)){
                productionRequest.setParameter(JobConfigNames.CALVALUS_SYSTEM_NAME, systemName);
            }
            if ("debug".equals(config.get("calvalus.crypt.auth"))) {
                String publicKey = config.get("calvalus.crypt.calvalus-public-key");
                String privateKey = config.get("calvalus.crypt.debug-private-key");
                String certificate = config.get("calvalus.crypt.debug-certificate");
                hook = new DebugTokenGenerator(publicKey, privateKey, certificate, getUserName());
            } else if ("saml".equals(config.get("calvalus.crypt.auth"))) {
                try {
                    HttpSession session = getThreadLocalRequest().getSession();
                    AssertionImpl assertion = (AssertionImpl) session.getAttribute("_const_cas_assertion_");
                    Document samlToken = (Document) assertion.getPrincipal().getAttributes().get("rawSamlToken");
                    samlToken = fixRootNode(samlToken);
                    String saml = getStringFromDoc(samlToken);
                    String publicKey = config.get("calvalus.crypt.calvalus-public-key");
                    hook = new TokenGenerator(publicKey, saml);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                }
            }
            HttpSession session = getThreadLocalRequest().getSession();
            Object requestSizeLimit = session.getAttribute(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT);
            if (requestSizeLimit != null) {
                productionRequest.setParameter(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT, (String) requestSizeLimit);
            }
            ProductionResponse productionResponse = serviceContainer.getProductionService().orderProduction(productionRequest, hook);
            return convert(productionResponse);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void saveRequest(DtoProductionRequest dtoProductionRequest) throws BackendServiceException {
        String userName = getUserName();
        ProductionRequest productionRequest = convert(dtoProductionRequest);
        WpsXmlRequestConverter xmlRequestConverter = new WpsXmlRequestConverter(productionRequest);
        try {
            String fileName = createRequestId(dtoProductionRequest.getProductionType());
            String userPath = AbstractFileSystemService.getUserPath(userName, "request/" + fileName);
            try (OutputStream out = new BufferedOutputStream(serviceContainer.getFileSystemService().addFile(userName, userPath), 64 * 1024)) {
                out.write(xmlRequestConverter.toXml().getBytes());
            }
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public void deleteRequest(String requestId) throws BackendServiceException {
        String userName = getUserName();
        String userPath = AbstractFileSystemService.getUserPath(userName, "request/" + requestId + REQUEST_FILE_EXTENSION);
        try {
            serviceContainer.getFileSystemService().removeFile(userName, userPath);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProductionRequest[] listRequests() throws BackendServiceException {
        String userName = getUserName();
        String userPath = AbstractFileSystemService.getUserPath(userName, "request/.*" + REQUEST_FILE_EXTENSION);
        try {
            FileSystemService fileSystemService = serviceContainer.getFileSystemService();
            String[] requestFilePaths = fileSystemService.globPaths(userName, Collections.singletonList(userPath));

            List<DtoProductionRequest> requests = new ArrayList<>();
            for (String requestFilePath : requestFilePaths) {
                try (InputStream is = fileSystemService.openFile(userName, requestFilePath)) {
                    Reader reader = new InputStreamReader(is);
                    ProductionRequest productionRequest = new WpsProductionRequestConverter(reader).loadProductionRequest(userName);
                    String requestId = extractRequestId(requestFilePath);
                    requests.add(convert(requestId, productionRequest));
                }
            }
            return requests.toArray(new DtoProductionRequest[requests.size()]);
        } catch (IOException | JDOMException e) {
            throw convert(e);
        }
    }

    private String extractRequestId(String requestFilePath) {
        String[] paths = requestFilePath.split("/");
        return paths[paths.length - 1].replace(REQUEST_FILE_EXTENSION, "");
    }

    @Override
    public void cancelProductions(String[] productionIds) throws BackendServiceException {
        try {
            serviceContainer.getProductionService().cancelProductions(productionIds);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void deleteProductions(String[] productionIds) throws BackendServiceException {
        try {
            serviceContainer.getProductionService().deleteProductions(productionIds);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void stageProductions(String[] productionIds) throws BackendServiceException {
        try {
            serviceContainer.getProductionService().stageProductions(productionIds);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void scpProduction(final String productionId, final String remotePath) throws BackendServiceException {
        try {
            serviceContainer.getProductionService().scpProduction(productionId, remotePath);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public String[] listUserFiles(String dirPath) throws BackendServiceException {
        try {
            String userName = getUserName();
            List<String> pathPatterns = Collections.singletonList(AbstractFileSystemService.getUserGlob(userName, dirPath));
            return serviceContainer.getFileSystemService().globPaths(userName, pathPatterns);
        } catch (IOException e) {
            throw convert(e);
        }
    }


    @Override
    public String[] listSystemFiles(String dirPath) throws BackendServiceException {
        try {
            String userName = getUserName();
            List<String> pathPatterns = Collections.singletonList(dirPath + "/.*");
            return serviceContainer.getFileSystemService().globPaths(userName, pathPatterns);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public boolean removeUserFile(String filePath) throws BackendServiceException {
        try {
            String userName = getUserName();
            String userPath = AbstractFileSystemService.getUserPath(userName, filePath);
            return serviceContainer.getFileSystemService().removeFile(userName, userPath);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public boolean removeUserFiles(String... filePaths) throws BackendServiceException {
        boolean deleted = true;
        for (String filePath : filePaths) {
            deleted = deleted && removeUserFile(filePath);
        }
        return deleted;
    }

    @Override
    public boolean removeUserDirectory(String filePath) throws BackendServiceException {
        try {
            String userName = getUserName();
            String userPath = AbstractFileSystemService.getUserPath(userName, filePath);
            return serviceContainer.getFileSystemService().removeDirectory(userName, userPath);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public String checkUserRecordSource(String filePath) throws BackendServiceException {
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(getUserName());
        try {
            return remoteUser.doAs((PrivilegedExceptionAction<String>) () -> {
                String url = serviceContainer.getFileSystemService().getQualifiedPath(getUserName(), filePath);
                RecordSourceSpi recordSourceSpi = RecordSourceSpi.getForUrl(url);
                RecordSource recordSource = recordSourceSpi.createRecordSource(url, serviceContainer.getHadoopConfiguration());
                Iterable<Record> records = recordSource.getRecords();
                int numRecords = 0;
                double latMin = +Double.MAX_VALUE;
                double latMax = -Double.MAX_VALUE;
                double lonMin = +Double.MAX_VALUE;
                double lonMax = -Double.MAX_VALUE;
                long timeMin = Long.MAX_VALUE;
                long timeMax = Long.MIN_VALUE;

                for (Record record : records) {
                    GeoPos location = record.getLocation();
                    if (location != null && location.isValid()) {
                        numRecords++;
                        latMin = Math.min(latMin, location.getLat());
                        latMax = Math.max(latMax, location.getLat());
                        lonMin = Math.min(lonMin, location.getLon());
                        lonMax = Math.max(lonMax, location.getLon());
                    }
                    Date time = record.getTime();
                    if (time != null) {
                        timeMin = Math.min(timeMin, time.getTime());
                        timeMax = Math.max(timeMax, time.getTime());
                    }
                }
                String reportMsg = String.format("%s. \nNumber of records with valid geo location: %d\n",
                        recordSource.getTimeAndLocationColumnDescription(),
                        numRecords);
                if (numRecords > 0) {
                    reportMsg += String.format("Latitude range: [%s, %s]\nLongitude range: [%s, %s]\n",
                            latMin, latMax, lonMin, lonMax);
                }
                if (timeMin != Long.MAX_VALUE && timeMax != Long.MIN_VALUE) {
                    reportMsg += String.format("Time range: [%s, %s]\n",
                            CCSDS_FORMAT.format(new Date(timeMin)),
                            CCSDS_FORMAT.format(new Date(timeMax)));
                } else {
                    reportMsg += "No time information given.\n";
                }
                return reportMsg.replace("\n", "<br>");
            });
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public float[] listUserRecordSource(String filePath) throws BackendServiceException {
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(getUserName());
        try {
            return remoteUser.doAs((PrivilegedExceptionAction<float[]>) () -> {
                String url = serviceContainer.getFileSystemService().getQualifiedPath(getUserName(), filePath);
                RecordSourceSpi recordSourceSpi = RecordSourceSpi.getForUrl(url);
                RecordSource recordSource = recordSourceSpi.createRecordSource(url, serviceContainer.getHadoopConfiguration());
                Iterable<Record> records = recordSource.getRecords();
                List<GeoPos> geoPoses = new ArrayList<>();
                for (Record record : records) {
                    GeoPos location = record.getLocation();
                    if (location != null && location.isValid()) {
                        geoPoses.add(location);
                    }
                }
                float[] latLons = new float[geoPoses.size() * 2];
                int i = 0;
                for (GeoPos geoPos : geoPoses) {
                    latLons[i++] = (float) geoPos.lat;
                    latLons[i++] = (float) geoPos.lon;
                }
                return latLons;
            });
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private String createRequestId(String productionType) {
        DateFormat dateFormat = DateUtils.createDateFormat("yyyyMMdd_HHmmssSSS");
        return dateFormat.format(new Date()) + "_" + productionType + REQUEST_FILE_EXTENSION;
    }

    private String[] convert(String[] strings) {
        if (strings == null) {
            return new String[0];
        } else {
            return strings;
        }
    }

    private DtoProductSet convert(ProductSet productSet) {
        return new DtoProductSet(productSet.getProductType(),
                productSet.getName(),
                productSet.getPath(),
                productSet.getMinDate(),
                productSet.getMaxDate(),
                productSet.getRegionName(),
                productSet.getRegionWKT(),
                productSet.getBandNames(),
                productSet.getGeoInventory());
    }

    private DtoProcessorDescriptor convert(String bundleName, String bundleVersion, String bundlePath, String owner,
                                           ProcessorDescriptor processorDescriptor) {
        return new DtoProcessorDescriptor(processorDescriptor.getExecutableName(),
                processorDescriptor.getProcessorName(),
                processorDescriptor.getProcessorVersion(),
                processorDescriptor.getDefaultParameters() != null ? processorDescriptor.getDefaultParameters().trim() : "",
                bundleName,
                bundleVersion,
                bundlePath,
                owner,
                processorDescriptor.getDescriptionHtml() != null ? processorDescriptor.getDescriptionHtml() : "",
                convert(processorDescriptor.getInputProductTypes()),
                convert(processorDescriptor.getProcessorCategory()),
                processorDescriptor.getOutputProductType(),
                convert(processorDescriptor.getOutputFormats()),
                processorDescriptor.getFormatting() != null ? processorDescriptor.getFormatting().toString() : "OPTIONAL",
                processorDescriptor.getMaskExpression(),
                convert(processorDescriptor.getOutputVariables()),
                convert(processorDescriptor.getParameterDescriptors()));
    }

    private DtoAggregatorDescriptor convert(String bundleName, String bundleVersion, String bundlePath, String owner,
                                            AggregatorDescriptor aggregatorDescriptor) {
        return new DtoAggregatorDescriptor(aggregatorDescriptor.getAggregator(),
                bundleName,
                bundleVersion,
                bundlePath,
                owner,
                aggregatorDescriptor.getDescriptionHtml() != null ? aggregatorDescriptor.getDescriptionHtml() : "",
                convert(aggregatorDescriptor.getParameterDescriptors()));
    }

    private DtoProcessorDescriptor.DtoProcessorCategory convert(ProcessorDescriptor.ProcessorCategory processorCategory) {
        if (processorCategory == null) {
            return DtoProcessorDescriptor.DtoProcessorCategory.LEVEL2;
        } else {
            return DtoProcessorDescriptor.DtoProcessorCategory.valueOf(processorCategory.name());
        }
    }

    private DtoParameterDescriptor[] convert(ProcessorDescriptor.ParameterDescriptor[] parameterDescriptors) {
        if (parameterDescriptors == null) {
            return new DtoParameterDescriptor[0];
        }
        DtoParameterDescriptor[] dtoParameterDescriptors = new DtoParameterDescriptor[parameterDescriptors.length];
        for (int i = 0; i < parameterDescriptors.length; i++) {
            ProcessorDescriptor.ParameterDescriptor parameterDescriptor = parameterDescriptors[i];
            dtoParameterDescriptors[i] = new DtoParameterDescriptor(parameterDescriptor.getName(),
                    parameterDescriptor.getType(),
                    parameterDescriptor.getDescription(),
                    parameterDescriptor.getDefaultValue(),
                    convert(parameterDescriptor.getValueSet()),
                    convert(parameterDescriptor.getValueRange()));
        }
        return dtoParameterDescriptors;
    }

    private DtoValueRange convert(String valueRangeAsText) {
        if (valueRangeAsText == null || valueRangeAsText.isEmpty()) {
            return null;
        } else {
            try {
                ValueRange vr = ValueRange.parseValueRange(valueRangeAsText);
                return new DtoValueRange(vr.getMin(), vr.getMax(), vr.isMinIncluded(), vr.isMaxIncluded());
            } catch (IllegalArgumentException iae) {
                log("Error while parsing 'valueRange' expression: " + valueRangeAsText, iae);
                return null;
            }
        }
    }

    private DtoProcessorVariable[] convert(ProcessorDescriptor.Variable[] outputVariables) {
        if (outputVariables == null) {
            return new DtoProcessorVariable[0];
        }
        DtoProcessorVariable[] processorVariables = new DtoProcessorVariable[outputVariables.length];
        for (int i = 0; i < outputVariables.length; i++) {
            ProcessorDescriptor.Variable outputVariable = outputVariables[i];
            processorVariables[i] = new DtoProcessorVariable(outputVariable.getName(),
                    outputVariable.getDefaultAggregator(),
                    outputVariable.getDefaultWeightCoeff());
        }
        return processorVariables;
    }

    private DtoProduction convert(Production production) {
        ProductionRequest productionRequest = production.getProductionRequest();
        String[] additionalStagingPaths = null;
        try {
            String additionalStagingPathsString = productionRequest.getParameter("additionalStagingPaths", false);
            if (additionalStagingPathsString != null) {
                additionalStagingPaths = additionalStagingPathsString.split(",");
            }
        } catch (ProductionException e) {
            log("Could not retrieve 'additionalStagingPaths' parameter.", e);
            additionalStagingPaths = null;
        }
        return new DtoProduction(production.getId(),
                production.getName(),
                productionRequest.getUserName(),
                productionRequest.getProductionType(),
                production.getWorkflow() instanceof HadoopWorkflowItem ? ((HadoopWorkflowItem) production.getWorkflow()).getOutputDir() : null,
                backendConfig.getStagingPath() + "/" + production.getStagingPath() + "/",
                convert(additionalStagingPaths),
                production.isAutoStaging(),
                convert(production.getProcessingStatus(), production.getWorkflow()),
                convert(production.getStagingStatus()));
    }

    private DtoProductionRequest convert(String requestId, ProductionRequest productionRequest) {
        return new DtoProductionRequest(requestId,
                productionRequest.getProductionType(),
                productionRequest.getParameters());
    }

    private DtoProcessStatus convert(ProcessStatus status, WorkflowItem workflow) {
        Date startTime = workflow.getStartTime();
        Date stopTime = workflow.getStopTime();
        int processingSeconds = 0;
        if (startTime != null) {
            if (stopTime == null) {
                stopTime = new Date();
            }
            processingSeconds = (int) ((stopTime.getTime() - startTime.getTime()) / 1000);
        }
        return new DtoProcessStatus(DtoProcessState.valueOf(status.getState().name()),
                status.getMessage(),
                status.getProgress(),
                processingSeconds);
    }

    private DtoProcessStatus convert(ProcessStatus status) {
        return new DtoProcessStatus(DtoProcessState.valueOf(status.getState().name()),
                status.getMessage(),
                status.getProgress());
    }

    private DtoProductionResponse convert(ProductionResponse productionResponse) {
        return new DtoProductionResponse(convert(productionResponse.getProduction()));
    }

    private ProductionRequest convert(DtoProductionRequest gwtProductionRequest) {
        return new ProductionRequest(gwtProductionRequest.getProductionType(),
                getUserName(),
                gwtProductionRequest.getProductionParameters());
    }

    private BackendServiceException convert(Exception e) {
        log(e.getMessage(), e);
        return new BackendServiceException(e.getMessage(), e);
    }

    private void initLogger(ServletContext servletContext) {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        logger.addHandler(new ServletContextLogHandler(servletContext));
    }

    private void initBackendConfig(ServletContext servletContext) throws ServletException {
        backendConfig = new BackendConfig(servletContext);
        logConfig();
    }

    private void logConfig() {
        log("Calvalus configuration loaded:");
        log("  local context dir          = " + backendConfig.getLocalContextDir());
        log("  local staging dir          = " + backendConfig.getLocalStagingDir());
        log("  local upload dir           = " + backendConfig.getLocalUploadDir());
        log("  staging path               = " + backendConfig.getStagingPath());
        log("  upload path                = " + backendConfig.getUploadPath());
        log("  production service factory = " + backendConfig.getProductionServiceFactoryClassName());
        log("  configuration:");
        Map<String, String> configMap = backendConfig.getConfigMap();
        Set<Map.Entry<String, String>> entries = configMap.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            log("    " + entry.getKey() + " = " + entry.getValue());
        }
    }

    private void initProductionService() throws ServletException {
        try {
            Class<?> productionServiceFactoryClass = Class.forName(
                    backendConfig.getProductionServiceFactoryClassName());
            ServiceContainerFactory serviceContainerFactory = (ServiceContainerFactory) productionServiceFactoryClass.newInstance();
            serviceContainer = serviceContainerFactory.create(backendConfig.getConfigMap(),
                    backendConfig.getLocalAppDataDir(),
                    backendConfig.getLocalStagingDir());
            // Make the production servlet accessible by other servlets:
            getServletContext().setAttribute("serviceContainer", serviceContainer);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void startObservingProductionService() {
        //statusObserver = new Timer("StatusObserver", true);
        Timer statusObserver2 = serviceContainer.getProductionService().getProcessingService().getTimer();
        statusObserver2.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateProductionStatuses();
            }
        }, PRODUCTION_STATUS_OBSERVATION_PERIOD, PRODUCTION_STATUS_OBSERVATION_PERIOD);
    }

    private void updateProductionStatuses() {
        final ProductionService productionService = this.serviceContainer.getProductionService();
        if (productionService != null) {
            productionService.updateStatuses(getUserName());
        }
    }

    private String getUserName() {
        return getUserName(getThreadLocalRequest());
    }

    static String getUserName(HttpServletRequest request) {
        if (request != null) {
            Principal userPrincipal = request.getUserPrincipal();
            if (userPrincipal != null) {
                return userPrincipal.getName();
            }
            String userName = request.getRemoteUser();
            if (userName != null) {
                return userName;
            }
        }
        return "anonymous";
    }

    @Override
    public DtoCalvalusConfig getCalvalusConfig() {
        List<String> samlRoles = new ArrayList<>();
        Map<String, String> userSpecificConfig = new HashMap<>(backendConfig.getConfigMap());
        if ("saml".equals(userSpecificConfig.get("calvalus.crypt.auth"))) {
            try {
                HttpSession session = getThreadLocalRequest().getSession();
                AssertionImpl assertion = (AssertionImpl) session.getAttribute("_const_cas_assertion_");
                Document samlToken = (Document) assertion.getPrincipal().getAttributes().get("rawSamlToken");
                samlToken = fixRootNode(samlToken);
                samlRoles = getGroupMemberships(samlToken);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        userSpecificConfig.put("user", getUserName());
        String[] configuredRoles;
        String roleParameter = userSpecificConfig.get(USER_ROLE);
        if (roleParameter != null && roleParameter.trim().length() > 0) {
            configuredRoles = roleParameter.trim().split(" ");
        } else {
            configuredRoles = new String[]{"calvalus"};
        }

        List<String> accu = new ArrayList<>();
        for (String role : configuredRoles) {
            boolean noRolesConfigured = roleParameter == null;
            if (noRolesConfigured || samlRoles.contains(role) || getThreadLocalRequest().isUserInRole(role)) {
                accu.add(role);
            }
        }

        userSpecificConfig.put("roles", accu.toString());

        String defaultSizeLimit = userSpecificConfig.get(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT);
        int requestSizeLimit = 0;
        if (defaultSizeLimit != null) {
            try {
                requestSizeLimit = Integer.parseInt(defaultSizeLimit);
            } catch (NumberFormatException e) {
                LOG.log(Level.WARNING, "Illegal value for property '" + JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT + "': " + defaultSizeLimit, e);
            }
        }
        for (String role : accu) {
            String key = JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT + "." + role;
            String roleRequestSizeLimit = userSpecificConfig.get(key);
            if (roleRequestSizeLimit != null) {
                int limit;
                try {
                    limit = Integer.parseInt(roleRequestSizeLimit);
                    if (limit == 0) {
                        requestSizeLimit = 0;
                        break;
                    }
                    if (requestSizeLimit < limit) {
                        requestSizeLimit = limit;
                    }
                } catch (NumberFormatException e) {
                    LOG.log(Level.WARNING, "Illegal value for property '" + key + "': " + roleRequestSizeLimit, e);
                }
            }
        }

        //userSpecificConfig.put(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT, "" + requestSizeLimit);

        HttpSession session = getThreadLocalRequest().getSession();
        session.setAttribute(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT, "" + requestSizeLimit);

        LOG.info("Roles of user '" + getUserName() + "': " + Arrays.toString(accu.toArray(new String[0])));
        LOG.info("requestSizeLimit of user '" + getUserName() + "': " + requestSizeLimit);

        return new DtoCalvalusConfig(getUserName(), accu.toArray(new String[accu.size()]), userSpecificConfig);
    }

    static Document fixRootNode(Document samlToken) throws org.jdom2.JDOMException {
        DOMBuilder builder = new DOMBuilder();
        org.jdom2.Document jDomDoc = builder.build(samlToken);
        Element assertionElement = jDomDoc.getRootElement().getChildren().get(0);
        jDomDoc.detachRootElement();
        assertionElement.detach();
        jDomDoc.setRootElement(assertionElement);
        DOMOutputter outputter = new DOMOutputter();
        return outputter.output(jDomDoc);
    }

    static List<String> getGroupMemberships(Document samlToken) throws XPathExpressionException {
        List<String> groupMemberships = new ArrayList<>();
        XPathFactory xPathfactory = XPathFactory.newInstance();
        XPath xpath = xPathfactory.newXPath();
        String expression =
                "/*[local-name()='Assertion']" +
                        "/*[local-name()='AttributeStatement']" +
                        "/*[local-name()='Attribute'][@Name=\"memberOf\"]" +
                        "/*[local-name()='AttributeValue']";
        XPathExpression expr = xpath.compile(expression);
        NodeList nodeList = (NodeList) expr.evaluate(samlToken, XPathConstants.NODESET);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node item = nodeList.item(i);
            groupMemberships.add(item.getTextContent().split(",")[0].split("=")[1]);
        }
        return groupMemberships;
    }


    @Override
    public String[][] calculateL3Periods(String minDate, String maxDate, String steppingPeriodLength, String compositingPeriodLength) {
        SimpleDateFormat dateFormat = DateUtils.createDateFormat("yyyy-MM-dd");
        Date start = null;
        Date end = null;
        try {
            start = dateFormat.parse(minDate);
            end = dateFormat.parse(maxDate);
        } catch (ParseException e) {
            log("Failed to parse date.", e);
            return new String[0][];
        }
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(start, end, steppingPeriodLength, compositingPeriodLength);
        String[][] periods = new String[dateRanges.size()][2];
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);
            periods[i][0] = dateFormat.format(dateRange.getStartDate());
            periods[i][1] = dateFormat.format(dateRange.getStopDate());
        }
        return periods;
    }

    @Override
    public DtoRegionDataInfo loadRegionDataInfo(String filePath) throws BackendServiceException {
        try {
            String userName = getUserName();
            String url = serviceContainer.getFileSystemService().getQualifiedPath(userName, filePath);
            String[][] data = serviceContainer.getProductionService().loadRegionDataInfo(userName, url);
            String[][] values = Arrays.copyOfRange(data, 1, data.length);
            String[] header = data[0];
            return new DtoRegionDataInfo(header, values);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    private String getStringFromDoc(Document doc) {
        try {
            DOMSource domSource = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.transform(domSource, result);
            writer.flush();
            return writer.toString();
        } catch (TransformerException ex) {
            throw new IllegalArgumentException("Unable to parse SAML token", ex);
        }
    }

}
