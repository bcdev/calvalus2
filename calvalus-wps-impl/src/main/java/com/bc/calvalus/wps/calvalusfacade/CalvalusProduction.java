package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.util.TokenGenerator;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.api.exceptions.MissingParameterValueException;
import com.bc.wps.api.schema.Execute;
import org.apache.xml.security.encryption.XMLCipher;
import org.apache.xml.security.encryption.XMLEncryptionException;
import org.apache.xml.security.utils.EncryptionConstants;
import org.apache.xpath.operations.Bool;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.DOMBuilder;
import org.jdom2.output.DOMOutputter;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles the order production operations (synchronously and asynchronously).
 *
 * @author hans
 */
class CalvalusProduction {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final int PRODUCTION_STATUS_OBSERVATION_PERIOD = 10000;

    LocalProductionStatus orderProductionAsynchronous(Execute executeRequest, String userName, CalvalusFacade calvalusFacade) throws WpsProductionException {
        try {
            ServiceContainer serviceContainer = CalvalusProductionService.getServiceContainerSingleton();
            ProductionRequest request = createProductionRequest(executeRequest, userName, serviceContainer, calvalusFacade);
            String casTgcString = calvalusFacade.getHeaderField("Cookie").split(";")[1];
            String casTgc = casTgcString.split("=")[1];
            Map<String, String> config = ProductionServiceConfig.loadConfig(CalvalusProductionService.getConfigFile(), null);
            return doProductionAsynchronous(request, serviceContainer.getProductionService(), userName, config, casTgc);
        } catch (ProductionException | IOException | InvalidParameterValueException | WpsProcessorNotFoundException |
                    MissingParameterValueException | InvalidProcessorIdException | JAXBException exception) {
            throw new WpsProductionException("Processing failed : " + exception.getMessage(), exception);
        }
    }

    LocalProductionStatus orderProductionSynchronous(Execute executeRequest, String userName, CalvalusFacade calvalusFacade) throws WpsProductionException {
        try {
            ServiceContainer serviceContainer = CalvalusProductionService.getServiceContainerSingleton();
            ProductionRequest request = createProductionRequest(executeRequest, userName, serviceContainer, calvalusFacade);
            String casTgcString = calvalusFacade.getHeaderField("Cookie").split(";")[1];
            String casTgc = casTgcString.split("=")[1];
            Map<String, String> config = ProductionServiceConfig.loadConfig(CalvalusProductionService.getConfigFile(), null);
            LocalProductionStatus status = doProductionSynchronous(serviceContainer.getProductionService(), request, config, casTgc);
            String jobId = status.getJobId();
            calvalusFacade.stageProduction(jobId);
            calvalusFacade.observeStagingStatus(jobId);
            status.setResultUrls(calvalusFacade.getProductResultUrls(jobId));
            status.setStopDate(new Date());
            status.setState(ProcessState.COMPLETED);
            if(Boolean.valueOf(config.get("calvalus.generate.metadata"))){
                calvalusFacade.generateProductMetadata(jobId);
            }
            return status;
        } catch (WpsResultProductException | JAXBException | MissingParameterValueException | InvalidProcessorIdException | WpsStagingException |
                    ProductMetadataException | InterruptedException | WpsProcessorNotFoundException | ProductionException |
                    InvalidParameterValueException | IOException exception) {
            throw new WpsProductionException("Processing failed : " + exception.getMessage(), exception);
        }
    }

    private LocalProductionStatus doProductionSynchronous(ProductionService productionService, ProductionRequest request, Map<String, String> config, String cookie)
                throws ProductionException, InterruptedException {
        logInfo("Ordering production...");

        TokenGenerator jobHook = createSamlTokenHook(config, cookie);
        ProductionResponse productionResponse = productionService.orderProduction(request, jobHook);
        Production production = productionResponse.getProduction();
        logInfo("Production successfully ordered. The production ID is: " + production.getId());
        observeProduction(productionService, production);
        ProcessStatus status = production.getProcessingStatus();
        return new LocalProductionStatus(production.getId(),
                status.getState(),
                status.getProgress(),
                status.getMessage(),
                null);
    }

    private LocalProductionStatus doProductionAsynchronous(ProductionRequest request, ProductionService productionService, String userName, Map<String, String> hadoopConfiguration, String cookie)
                throws ProductionException {
        logInfo("Ordering production...");
        logInfo("user : " + userName);
        logInfo("request user name : " + request.getUserName());

        TokenGenerator samlTokenHook = createSamlTokenHook(hadoopConfiguration, cookie);

        ProductionResponse productionResponse = productionService.orderProduction(request, samlTokenHook);
        Production production = productionResponse.getProduction();
        logInfo("Production successfully ordered. The production ID is: " + production.getId());

        Timer statusObserver = CalvalusProductionService.getStatusObserverSingleton();
        synchronized (CalvalusProductionService.getUserProductionMap()) {
            if (!CalvalusProductionService.getUserProductionMap().containsKey(userName)) {
                CalvalusProductionService.getUserProductionMap().put(userName, 1);
                statusObserver.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            updateProductionStatuses(userName);
                        } catch (IOException | ProductionException e) {
                            LOG.log(Level.SEVERE, "Unable to update production status.", e);
                        }
                    }
                }, PRODUCTION_STATUS_OBSERVATION_PERIOD, PRODUCTION_STATUS_OBSERVATION_PERIOD);
            }
        }

        ProcessStatus status = production.getProcessingStatus();
        return new LocalProductionStatus(production.getId(),
                                         status.getState(),
                                         status.getProgress(),
                                         status.getMessage(),
                                         null);

    }

    private ProductionRequest createProductionRequest(Execute executeRequest, String userName,
                                                      ServiceContainer serviceContainer, CalvalusFacade calvalusFacade)
                throws MissingParameterValueException, InvalidParameterValueException, JAXBException,
                       InvalidProcessorIdException, WpsProcessorNotFoundException, IOException, ProductionException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);
        String processorId = executeRequest.getIdentifier().getValue();
        ProcessorNameConverter parser = new ProcessorNameConverter(processorId);
        WpsProcess calvalusProcessor = calvalusFacade.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, calvalusProcessor,
                                                                       getProductSets(userName, serviceContainer),
                                                                       calvalusFacade.getRequestHeaderMap());
        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     userName,
                                     calvalusDataInputs.getInputMapFormatted());
    }

    private ProductSet[] getProductSets(String userName, ServiceContainer serviceContainer) throws ProductionException, IOException {
        List<ProductSet> productSets = new ArrayList<>();
        productSets.addAll(Arrays.asList(serviceContainer.getInventoryService().getProductSets(userName, "")));
        productSets.addAll(Arrays.asList(serviceContainer.getInventoryService().getProductSets(userName, "user=" + userName)));
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    private void observeProduction(ProductionService productionService, Production production) throws InterruptedException {
        final Thread shutDownHook = createShutdownHook(production.getWorkflow());
        Runtime.getRuntime().addShutdownHook(shutDownHook);

        String userName = production.getProductionRequest().getUserName();
        while (!production.getProcessingStatus().getState().isDone()) {
            Thread.sleep(5000);
            productionService.updateStatuses(userName);
            ProcessStatus processingStatus = production.getProcessingStatus();
            logInfo(String.format("Production remote status: state=%s, progress=%s, message='%s'",
                                  processingStatus.getState(),
                                  processingStatus.getProgress(),
                                  processingStatus.getMessage()));
        }
        Runtime.getRuntime().removeShutdownHook(shutDownHook);

        if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
            logInfo("Production completed. Output directory is " + production.getStagingPath());
        } else {
            logError("Error: Production did not complete normally: " + production.getProcessingStatus().getMessage());
        }
    }


    private void updateProductionStatuses(String userName) throws IOException, ProductionException {
        final ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
        if (productionService != null) {
            synchronized (this) {
                try {
                    productionService.updateStatuses(userName);
                } catch (IllegalStateException exception) {
                    System.out.println("Trying to stop thread " + Thread.currentThread().getName());
                    Timer statusObserver = CalvalusProductionService.getStatusObserverSingleton();
                    statusObserver.cancel();
                }
            }
        }
    }

    private Thread createShutdownHook(final WorkflowItem workflow) {
        return new Thread(() -> {
            try {
                workflow.kill();
            } catch (Exception e) {
                logError("Failed to shutdown production: " + e.getMessage());
            }
        });
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }

    private TokenGenerator createSamlTokenHook(Map<String, String> config, String cookie) throws ProductionException {
        String publicKey = config.get("calvalus.crypt.calvalus-public-key");
        String casUrl = config.get("calvalus.cas.url");
        String privateKeyPath = config.get("calvalus.crypt.samlkey-private-key");
        String serviceUrl = config.get("calvalus.cas.serviceUrl");

        String samlToken;
        try {
            samlToken = fetchSamlToken(cookie, casUrl, serviceUrl, privateKeyPath);
        } catch (IOException | NoSuchAlgorithmException | NoSuchProviderException | ParserConfigurationException | XMLEncryptionException | InvalidKeySpecException | JDOMException | SAXException e) {
            throw new ProductionException("Error fetching SAML token, see nested exception", e);
        }
        return new TokenGenerator(publicKey, samlToken);
    }

    private String fetchSamlToken(String tgc, final String casUrl, String serviceUrl, String privateKeyPath) throws IOException, NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException, ParserConfigurationException, SAXException, JDOMException, XMLEncryptionException {
        org.apache.xml.security.Init.init();
        String urlString = casUrl + "/samlCreate2?service=" + URLEncoder.encode(serviceUrl, "UTF-8");
        String originalHttpAgent = System.getProperty("http.agent", "");
        System.setProperty("http.agent", "");
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Cookie", "CASTGC=" + tgc);
        conn.setRequestProperty("User-Agent", "curl/7.29.0");
        conn.setUseCaches(false);
        StringBuilder saml = new StringBuilder();
        try (InputStream in = conn.getInputStream()) {
            int c;
            while ((c = in.read()) > 0) {
                saml.append((char) c);
            }
        }

        System.setProperty("http.agent", originalHttpAgent);

        PrivateKey privateSamlKey = readPrivateDerKey(privateKeyPath);
        Document document = parseXml(saml.toString());
        document = decipher(privateSamlKey, document);
        document = fixRootNode(document);
        return getStringFromDoc(document);
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

    private static String getStringFromDoc(Document doc) {
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

    private static PrivateKey readPrivateDerKey(String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchProviderException {
        byte[] privKeyByteArray = Files.readAllBytes(Paths.get(filename));
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privKeyByteArray);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    private static Document parseXml(String xml) throws SAXException, IOException, ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes());
        return db.parse(inputStream);
    }

    private static Document decipher(PrivateKey myPrivKey, Document document) throws XMLEncryptionException {
        org.w3c.dom.Element encryptedDataElement = (org.w3c.dom.Element) document.getElementsByTagNameNS(EncryptionConstants.EncryptionSpecNS, EncryptionConstants._TAG_ENCRYPTEDDATA).item(0);
        XMLCipher xmlCipher = XMLCipher.getInstance();
        xmlCipher.init(XMLCipher.DECRYPT_MODE, null);
        xmlCipher.setKEK(myPrivKey);
        try {
            return xmlCipher.doFinal(document, encryptedDataElement);
        } catch (Exception e) {
            throw new XMLEncryptionException("", e);
        }
    }
}
