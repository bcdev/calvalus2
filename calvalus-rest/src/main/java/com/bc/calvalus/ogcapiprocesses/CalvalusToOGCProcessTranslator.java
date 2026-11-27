package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.ogcapiprocesses.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;

public class CalvalusToOGCProcessTranslator {
    private static final Logger LOG = CalvalusLogger.getLogger();
    public OGCProcessDescription toOgcProcessDescription(CalvalusProcessorDescriptor calvalusDescriptor) {
        return toOgcProcessDescription(calvalusDescriptor, null);
    }

    public OGCProcessDescription toOgcProcessDescription(CalvalusProcessorDescriptor calvalusDescriptor, String processId) {
        OGCProcessDescription ogcProcessDescription = new OGCProcessDescription();

        HashMap<String, OGCInputDescription> inputDescriptions = new HashMap<>();
        for (String key: calvalusDescriptor.parameterDescriptor.keySet()) {
            CalvalusParameterDescriptor parameterEntry = calvalusDescriptor.parameterDescriptor.get(key);
            OGCInputDescription input = new OGCInputDescription();

            input.schema = createSchemaFromParameter(parameterEntry.type, parameterEntry.defaultValue);
            inputDescriptions.put(key, input);
        }


        OGCInputDescription inputProductDescription = new OGCInputDescription();
        inputProductDescription.schema = productInputSchema();
        inputDescriptions.put("inputProduct", inputProductDescription);

        OGCInputDescription outputDirDescription = new OGCInputDescription();
        outputDirDescription.schema = outputDirInputSchema();
        inputDescriptions.put("outputDir", outputDirDescription);

        // TODO still a dummy implementation
        OGCOutputDescription outputDescription = new OGCOutputDescription();
        ObjectNode outputSchema = JsonNodeFactory.instance.objectNode();
        //for (String outputBandName: calvalusDescriptor.outputBandDescriptor.keySet()) {
        //}

        ObjectNode outputProductSchema = outputSchema.putObject("outputProduct");
        outputProductSchema.put("title", "output");
        outputDescription.schema = outputSchema;
        HashMap<String, OGCOutputDescription> outputDescriptions = new HashMap<>();
        outputDescriptions.put("output", outputDescription);

        ogcProcessDescription.id = processId;
        ogcProcessDescription.version = calvalusDescriptor.version;

        ogcProcessDescription.inputs = inputDescriptions;
        ogcProcessDescription.outputs = outputDescriptions;
        ogcProcessDescription.title = calvalusDescriptor.title;
        ogcProcessDescription.description = calvalusDescriptor.description;
        return ogcProcessDescription;
    }

    /**
     * Converts types specified in Calvalus processor descriptors to those allowed in OGC Process Descriptor Schemas:
     * <a href="http://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/schema.yaml">...</a>
     * @param type Type as declared in the Calvalus processor descriptor
     * @return a valid OGC schema type or null if none was found
     */
    public String convertCalvalusToOgcSchemaType(String type) {
        switch (type) {
            case "array":
                return "array";
            case "boolean":
                return "boolean";
            case "integer":
            case "int":
                return "integer";
            case "number":
            case "float":
                return "number";
            case "object":
                return "object";
            case "string":
            case "enum":
                return "string";
            default:
                return null;
        }
    }

    private ObjectNode createSchemaFromParameter(String type, String defaultValue) {
        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode schema = objectMapper.createObjectNode();
        String schemaType = convertCalvalusToOgcSchemaType(type);
        schema.put("type", schemaType);
        if (defaultValue != null) {
            schema.set("default", convert(TextNode.valueOf(defaultValue), schemaType));
        }

        return schema;
    }

    public static JsonNode convert(JsonNode value, String type) {
        if (value == null || value.isNull() || type == null) {
            return value;
        }
        try {
            switch (type) {
                case "integer":
                    return IntNode.valueOf(Integer.parseInt(value.asText()));
                case "number":
                    return DoubleNode.valueOf(Double.parseDouble(value.asText()));
                case "boolean":
                    return BooleanNode.valueOf(Boolean.parseBoolean(value.asText()));
                default:
                    return value;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Default value '" + value.asText() + "' is not valid for type '" + type + "'", e
            );
        }
    }

    /**
     * Constructs the schema for calvalus product input.
     * The schema is static, the "$ref" entries in the schema are resolved.
     * @return OpenAPI schema for calvalus product selection
     */
    public static JsonNode productInputSchema() {
        // TODO move out of method
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        JsonNode schema;
        try (InputStream in = OpenApiRefResolver.class.getResourceAsStream("/schemas/input-product.yml")) {
            JsonNode root = yaml.readTree(in);
            // set input collections
            // ArrayNode collectionsNode = ((ObjectNode) root).putArray("collections");
            // collections.forEach(collectionsNode::add);

             schema = OpenApiRefResolver.resolve(root.at("/calvalus_input/schema"), root);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return schema;
    }

    /**
     *
     * @param inputs
     * @return
     */
    public static HashMap<String, Object> productInputsToParameter(Map<String, Object> inputProduct) {
        if (inputProduct == null) {
            throw new IllegalArgumentException("inputProduct is null");
        }
        final boolean isMapTemporalProductInput = inputProduct.get("bbox") != null;
        if (!isMapTemporalProductInput) {
            throw new NotImplementedException("Only Map + Time range is supported as input product");
        }

        // Map + time range
        ArrayList<Number> bbox = (ArrayList<Number>) inputProduct.get("bbox");
        double[] bboxArray = new double[bbox.size()];
        for (int i = 0; i < bboxArray.length; i++) {
            bboxArray[i] = bbox.get(i).doubleValue();
        }
        String wkt = bbox2Wkt(bboxArray);

        final Map<String, Object> timeRangeNode = (Map<String, Object>) inputProduct.get("time_range");
        final String startTime = timeRangeNode.get("start_time").toString().substring(0, DateUtils.DATE_FORMAT_STRING.length());
        final String stopTime = timeRangeNode.get("stop_time").toString().substring(0, DateUtils.DATE_FORMAT_STRING.length());

        final String dateRanges = "[" + startTime + ":" + stopTime + "]";
        LOG.info("dateRanges: " + dateRanges);
        HashMap<String, Object> calvalusInputParameters = new HashMap<>();
        calvalusInputParameters.put("regionGeometry", wkt);
        calvalusInputParameters.put("dateRanges", dateRanges);

        return calvalusInputParameters;
    }
    // TODO find pre-made tool for this
    private static String bbox2Wkt(double[] bbox) {
        double MinX = bbox[0];
        double MinY = bbox[1];
        double MaxX = bbox[2];
        double MaxY = bbox[3];
        return String.format(
                Locale.ROOT,
                "POLYGON((%s %s, %s %s, %s %s, %s %S, %s %s))",
                MinX,
                MinY,
                MaxX,
                MinY,
                MaxX,
                MaxY,
                MinX,
                MaxY,
                MinX,
                MinY
            );
    }

    /**
     * outputDir is a parameter that needs to be set for any processing job and is not specified in the
     * processor descriptor.
     * @return schema for outputDir
     */
    public static JsonNode outputDirInputSchema() {
        // TODO move out of method
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        JsonNode schema;
        try (InputStream in = OpenApiRefResolver.class.getResourceAsStream("/schemas/output-dir.yml")) {
            schema = yaml.readTree(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return schema;
    }

    public static OGCJobStatus getOgcJobStatus(JobStatus hadoopJobStatus) {
        JobStatus.State hadoopJobStatusEnum = hadoopJobStatus.getState();
        switch (hadoopJobStatusEnum) {
            case RUNNING:
                return OGCJobStatus.RUNNING;
            case SUCCEEDED:
                return OGCJobStatus.SUCCESSFUL;
            case FAILED:
                return OGCJobStatus.FAILED;
            case PREP:
                return OGCJobStatus.ACCEPTED;
            case KILLED:
                return OGCJobStatus.DISMISSED;
        }

        return null;
    }
}
