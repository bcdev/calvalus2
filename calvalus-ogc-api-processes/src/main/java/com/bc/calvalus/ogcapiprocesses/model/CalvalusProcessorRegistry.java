package com.bc.calvalus.ogcapiprocesses.model;

import java.io.File;
import java.util.HashMap;
import java.util.Optional;

public class CalvalusProcessorRegistry {
    private final HashMap<String, CalvalusProcessorRegistryEntry> processIdMap;
    public CalvalusProcessorRegistry() {
        processIdMap = new HashMap<>();
        processIdMap.put("c2rcc-olci", new CalvalusProcessorRegistryEntry("c2rcc-14.0", "c2rcc-olci"));
    }

    public Optional<String> fullProcessorId(String processId) {
        return Optional.ofNullable(this.processIdMap.get(processId).fullProcessorId());
    }

    public Optional<String> relativeDescriptorPathString(String processId) {
        return Optional.ofNullable(this.processIdMap.get(processId).relativeDescriptorPathString());
    }

    public Optional<File> relativeDescriptorPath(String processId) {
        Optional<String> path = relativeDescriptorPathString(processId);
        return path.map(File::new);
    }
}

class CalvalusProcessorRegistryEntry {
    public static final String DESCRIPTOR_SUFFIX = "-descriptor.json";
    public String bundle;
    public String processor;

    public CalvalusProcessorRegistryEntry(String bundle, String processor) {
        this.bundle = bundle;
        this.processor = processor;
    }

    public String fullProcessorId() {
        return this.bundle + File.separator + this.processor;
    }
    public String relativeDescriptorPathString() {
        return fullProcessorId() + DESCRIPTOR_SUFFIX;
    }
}