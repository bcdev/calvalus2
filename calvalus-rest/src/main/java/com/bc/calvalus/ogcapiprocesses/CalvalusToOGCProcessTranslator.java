package com.bc.calvalus.ogcapiprocesses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusParameterDescriptor;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusProcessorDescriptor;
import com.bc.calvalus.ogcapiprocesses.model.OGCInputDescription;
import com.bc.calvalus.ogcapiprocesses.model.OGCOutputDescription;
import com.bc.calvalus.ogcapiprocesses.model.OGCProcessDescription;

import java.util.HashMap;

public class CalvalusToOGCProcessTranslator {
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

        OGCInputDescription inputProductTypeDescription = new OGCInputDescription();
        ObjectNode inputProductTypeDescriptionSchema = JsonNodeFactory.instance.objectNode();
        inputProductTypeDescriptionSchema.put("type", "string");
        ArrayNode inputTypeEnum = inputProductTypeDescriptionSchema.putArray("enum");

        for (String inputProductType: calvalusDescriptor.inputProductTypes) {
            inputTypeEnum.add(inputProductType);
        }
        inputProductTypeDescriptionSchema.put("default", calvalusDescriptor.inputProductTypes.get(0));
        inputProductTypeDescription.schema =  inputProductTypeDescriptionSchema;
        inputDescriptions.put("productType", inputProductTypeDescription);

        // TODO remove temporary hardcoded inputs
        ObjectNode outputPathSchema = JsonNodeFactory.instance.objectNode();
        outputPathSchema.put("type", "string");
        OGCInputDescription outputPathDescription = new OGCInputDescription();
        outputPathDescription.schema = outputPathSchema;
        // inputDescriptions.put("outputPath", outputPathDescription);
        inputDescriptions.put("outputDir", outputPathDescription);

        ObjectNode inputPathSchema = JsonNodeFactory.instance.objectNode();
        inputPathSchema.put("type", "string");
        OGCInputDescription inputPathDescription = new OGCInputDescription();
        inputPathDescription.schema = outputPathSchema;
        inputDescriptions.put("inputPath", inputPathDescription);

        ObjectNode inputGeometrySchema = JsonNodeFactory.instance.objectNode();
        ObjectNode inputGeometryItemType = JsonNodeFactory.instance.objectNode();
        inputGeometryItemType.put("type", "number");
        inputGeometrySchema.put("type", "array");
        inputGeometrySchema.set("items", inputGeometryItemType);
        inputGeometrySchema.put("minItems", 4);
        inputGeometrySchema.put("maxItems", 4);
        inputGeometrySchema.put("format", "bbox");
        inputGeometrySchema.put("x-ui-widget", "map");
        OGCInputDescription inputGeometryDescription = new OGCInputDescription();
        inputGeometryDescription.schema = inputGeometrySchema;
        inputDescriptions.put("inputGeometry", inputGeometryDescription);

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
}
