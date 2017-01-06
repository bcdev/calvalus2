package com.bc.calvalus.wps.utils;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

/**
 * @author hans
 */
public class VelocityWrapper {

    private VelocityContext context;

    private final VelocityEngine velocityEngine;

    public VelocityWrapper() {
        this.velocityEngine = new VelocityEngine();
        this.context = new VelocityContext();
        initDefaultVelocityEngine();
    }

    public VelocityWrapper(Properties properties) {
        this.velocityEngine = new VelocityEngine(properties);
        this.context = new VelocityContext();
        velocityEngine.init();
    }

    public String merge(Map<String, Object> contextMap, String templateFileName) {
        updateContext(contextMap);
        Template template = velocityEngine.getTemplate(templateFileName);
        StringWriter writer = new StringWriter();
        template.merge(context, writer);
        return writer.toString();
    }

    VelocityEngine getVelocityEngine() {
        return velocityEngine;
    }

    private void updateContext(Map<String, Object> contextMap) {
        for (String key : contextMap.keySet()) {
            context.put(key, contextMap.get(key));
        }
    }

    private void initDefaultVelocityEngine() {
        velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        velocityEngine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.extractor.SimpleLog4JLogSystem");
        velocityEngine.setProperty("runtime.extractor.logsystem.log4j.category", "velocity");
        velocityEngine.setProperty("runtime.extractor.logsystem.log4j.logger", "velocity");
        velocityEngine.init();
    }
}
