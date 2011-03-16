package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generates the WPS XML.
 */
@Deprecated
class WpsXmlGenerator {
    private final VelocityEngine velocityEngine;

    WpsXmlGenerator() throws ProductionException {
        this.velocityEngine = new VelocityEngine();
        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        try {
            velocityEngine.init(properties);
        } catch (Exception e) {
            throw new ProductionException(String.format("Failed to initialise Velocity engine: %s", e.getMessage()), e);
        }
    }

    public String createL2WpsXml(L2ProcessingRequest l2ProcessingRequest) throws ProductionException {
        // todo - write level2-wps-request.xml.vm
        return interpolateTemplate("com/bc/calvalus/production/hadoop/level2-wps-request.xml.vm",
                                   l2ProcessingRequest.getProcessingParameters());
    }

    /**
     * Creates WPS XML for the given Level 3 processing request.
     * The following parameters are expected to be present:
     * <pre>
     *
     * </pre>
     *
     * @param productionId      A production ID.
     * @param productionName    A production name.
     * @param processingRequest The L3 processing request.  @return The WPS XML plain text.
     * @throws ProductionException If the WPS XML cannot be created.
     */
    public String createL3WpsXml(String productionId,
                                 String productionName,
                                 L3ProcessingRequest processingRequest) throws ProductionException {
        Map<String, Object> templateParameters = new HashMap<String, Object>(processingRequest.getProcessingParameters());
        templateParameters.put("productionId", productionId);
        templateParameters.put("productionName", productionName);
        return interpolateTemplate("com/bc/calvalus/production/hadoop/level3-wps-request.xml.vm",
                                   templateParameters);
    }

    private String interpolateTemplate(String templatePath, Map<String, Object> templateParameters) throws ProductionException {
        VelocityContext context = new VelocityContext(templateParameters);
        String wpsXml;
        try {
            Template wpsXmlTemplate = velocityEngine.getTemplate(templatePath);
            StringWriter writer = new StringWriter();
            wpsXmlTemplate.merge(context, writer);
            wpsXml = writer.toString();
        } catch (Exception e) {
            throw new ProductionException("Failed to generate WPS XML request", e);
        }
        return wpsXml;
    }
}
