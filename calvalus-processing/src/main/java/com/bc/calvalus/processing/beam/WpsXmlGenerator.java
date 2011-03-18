package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.WorkflowException;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

/**
 * Generates the WPS XML.
 */
@Deprecated
public class WpsXmlGenerator {
    private final VelocityEngine velocityEngine;

    public WpsXmlGenerator() throws WorkflowException {
        this.velocityEngine = new VelocityEngine();
        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        try {
            velocityEngine.init(properties);
        } catch (Exception e) {
            throw new WorkflowException(String.format("Failed to initialise Velocity engine: %s", e.getMessage()), e);
        }
    }

    /**
     * Creates WPS XML for the given Level 3 processing request.
     * The following parameters are expected to be present:
     * <pre>
     *
     * </pre>
     *
     * @param templateParameters The L3 processing parameters.
     * @return The WPS XML plain text.
     * @throws WorkflowException If the WPS XML cannot be created.
     */
    public String createL3WpsXml(Map<String, Object> templateParameters) throws WorkflowException {
        return interpolateTemplate("com/bc/calvalus/processing/hadoop/level3-wps-request.xml.vm",
                                   templateParameters);
    }

    private String interpolateTemplate(String templatePath, Map<String, Object> templateParameters) throws WorkflowException {
        VelocityContext context = new VelocityContext(templateParameters);
        String wpsXml;
        try {
            Template wpsXmlTemplate = velocityEngine.getTemplate(templatePath);
            StringWriter writer = new StringWriter();
            wpsXmlTemplate.merge(context, writer);
            wpsXml = writer.toString();
        } catch (Exception e) {
            throw new WorkflowException("Failed to generate WPS XML request", e);
        }
        return wpsXml;
    }
}
