package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusFullProcessorDescriptor;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusProcessorRegistry;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Repository of processes deployed on calvalus.
 */
public class CalvalusProcessorRepository {
    private static final String SYSTEM_PATH = "/calvalus/software/1.0";
    private static final String USER_HOME = "/calvalus/home";
    private Logger LOG = CalvalusLogger.getLogger();
    private final String serviceDir;
    private final CalvalusProcessorRegistry registry;
    private final ObjectMapper objectMapper;

    public CalvalusProcessorRepository(String serviceDir, CalvalusProcessorRegistry registry) {
        this.serviceDir = serviceDir;
        this.registry = registry;
        this.objectMapper = new ObjectMapper();
    }

    public Optional<CalvalusFullProcessorDescriptor> getProcessorById(String processId) {
        Optional<File> descriptorPath = localDescriptorPath(processId);
        if(descriptorPath.isPresent()) {
            try {
                return Optional.ofNullable(objectMapper.readValue(descriptorPath.get(), CalvalusFullProcessorDescriptor.class));
            } catch (IOException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return Optional.empty();
    }

    public Optional<String> localDescriptorPathString(String processId) {
        Optional<String> relativePath = registry.relativeDescriptorPathString(processId);
        return relativePath.map(p -> serviceDir + File.separator + p);
    }

    public Optional<String> descriptorContent(String processId) {
        Optional<File> path = localDescriptorPath(processId);
        if (!path.isPresent()) {
            return Optional.empty();
        }
        try {
            String content = new String(Files.readAllBytes(path.get().toPath()), StandardCharsets.UTF_8);
            return Optional.of(content);
        } catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return Optional.empty();
        }
    }

    public Optional<File> localDescriptorPath(String processId) {
        return localDescriptorPathString(processId).map(File::new);
    }

    // TODO race condition on write
    public boolean refreshCache(CalvalusHadoopConnection hadoopConnection, String processId, String username) {
        Optional<String> processorFullId = this.registry.fullProcessorId(processId);
        if (! processorFullId.isPresent()) {
            return false;
        }
        File localPath = localDescriptorPath(processId).orElseThrow(RuntimeException::new);

        try (PrintStream out = new PrintStream(localPath)) {
            String processorDescriptorContent = hadoopConnection.getProcessorDescriptor(processorFullId.get(), username);
            out.print(processorDescriptorContent);
        } catch (IOException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }

        return true;
    }

    // TODO no need to re-read file
    public boolean checkAccess(String processId, DescriptorUtils.RoleMatcher roleMatcher) {
        Optional<File> path = localDescriptorPath(processId);
        Optional<String> content = descriptorContent(processId);
        if (! path.isPresent() || !content.isPresent()) {
            return false;
        }
        try {
            return roleMatcher.matches(content.get());
        } catch (JsonProcessingException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return false;
        }
    }
}
