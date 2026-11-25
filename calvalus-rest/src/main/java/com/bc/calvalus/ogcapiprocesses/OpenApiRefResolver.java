package com.bc.calvalus.ogcapiprocesses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Utility class to resolve "$ref" entries in OpenAPI schemas.
 * This is a proof-of-concept implementation with limited functionality (only references in the same file are supported)
 * @author Hannes Neuschmidt
 */
public class OpenApiRefResolver {
    public static JsonNode resolve(JsonNode node, JsonNode root) {
        if (node.isObject()) {
            JsonNode ref = node.get("$ref");
            if (ref != null) {
                // Strip leading "#", we only support the same file
                if (! ref.asText().startsWith("#")) {
                    throw new IllegalArgumentException("Ref '" + ref.asText() + "' is not relative to its own document (it doesn't start with '#')");
                }
                JsonNode target = root.at(ref.asText().substring(1));
                if (target.isMissingNode()) {
                    throw new IllegalArgumentException("Ref '" + ref.asText() + "' not found");
                }
                return resolve(target, root);
            }
            ObjectNode out = JsonNodeFactory.instance.objectNode();
            node.fields().forEachRemaining(entry -> out.set(entry.getKey(), resolve(entry.getValue(), root)));
            return out;
        } else if (node.isArray()) {
            ArrayNode out = JsonNodeFactory.instance.arrayNode();
            node.forEach(item -> out.add(resolve(item, root)));
            return out;
        }
        return node;
    }
}
