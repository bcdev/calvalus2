package com.bc.calvalus.production.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class DescriptorUtils {

    public static class IdMatcher {
        private String[] names = null;
        public IdMatcher(String names) {
            if (names != null) {
                this.names = names.split(",");
            }
        }
        public boolean matches(String id) {
            return names == null || Arrays.stream(names).anyMatch(x -> id.contains(x));
        }
    }

    public static class RoleMatcher {
        private String user = null;
        private List<String> roles = null;

        public RoleMatcher(String user, String[] roles) {
            this.user = user;
            this.roles = Arrays.asList(roles);
        }

        public boolean matches(File dir, String descriptorFilename) throws IOException {
            final StringBuilder accu = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(new File(dir, descriptorFilename)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    accu.append(line);
                    accu.append("\n");
                }
            }
            return matches(accu.toString());
        }

        public boolean matches(String content) throws JsonProcessingException {
            Map<String, Object> contentMap = parseRequest(content);
            List<String> authorisations = (List<String>) ((Map<String, Object>) contentMap.get("processorDescriptor")).get("authorisation");
            return matches(authorisations);
        }

        public boolean matches(List<String> authorisations) throws JsonProcessingException {
            for (String authorisation : authorisations) {
                if (authorisation.startsWith("group:")) {
                    if (roles.contains(authorisation.substring("group:".length()))) {
                        return true;
                    }
                } else if (authorisation.startsWith("user:")) {
                    if (authorisation.substring("user:".length()).equals(user)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {
    };

    public static Map<String, Object> parseRequest(String requestString) throws JsonProcessingException {
        final ObjectMapper jsonParser = new ObjectMapper();
        jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        return jsonParser.readValue(requestString, VALUE_TYPE_REF);
    }
}
