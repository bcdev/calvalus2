package com.bc.calvalus.api.ogcapiprocesses;

import com.bc.calvalus.ogcapiprocesses.OpenApiRefResolver;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OpenApiRefResolverTest {
    private static final String SCHEMA = String.join(System.lineSeparator(),
        "object_with_ref:",
        "   haystack:",
        "      $ref: \"#/path/to/value\"",
        "path:",
            "   to:",
            "       value: \"needle\""
    );
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void testResolve() {
        try {
            JsonNode root = yamlMapper.readTree(SCHEMA);
            JsonNode resolved = OpenApiRefResolver.resolve(root.at("/object_with_ref"), root);
            JsonNode actual = resolved.get("haystack");
            assertEquals("needle", actual.asText());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}