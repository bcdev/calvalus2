package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.ogcapiprocesses.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
            // TODO hardcoded values
            input.minOccurs = 1;
            input.maxOccurs = 1;

            input.schema = createSchemaFromParameter(parameterEntry.type, parameterEntry.defaultValue);
            inputDescriptions.put(key, input);
        }

        for (String inputProductType: calvalusDescriptor.inputProductTypes) {
            OGCInputDescription inputDescription = new OGCInputDescription();
            inputDescription.schema = createSchemaFromInputProductType(inputProductType);
            inputDescriptions.put("inputProduct", inputDescription);
        }

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
            schema.put("default", defaultValue);
        }

        return schema;
    }

    // TODO empty for now
    private ObjectNode createSchemaFromInputProductType(String productType) {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        return schema;
    }
}
