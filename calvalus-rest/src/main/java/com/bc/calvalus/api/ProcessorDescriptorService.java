package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.sun.jersey.api.NotFoundException;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("processor-descriptors")
public class ProcessorDescriptorService {

    private static Logger LOG = CalvalusLogger.getLogger();

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response list(
            @QueryParam("names") String names,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String maxAge = request.getHeader("max-age");
            final CalvalusHadoopConnection.IdMatcher idMatcher = new CalvalusHadoopConnection.IdMatcher(names);

            if ("0".equals(maxAge)) {
                final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
                final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
                CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(calvalusConfigPath);
                hadoopConnection.createJobClient(hadoopParameters);
                StringBuilder accu = new StringBuilder("[");
                hadoopConnection.stageProcessorDescriptors(username, serviceDir, idMatcher, accu);
                hadoopConnection.stageProcessorDescriptors(null, serviceDir, idMatcher, accu);
                accu.append("]");
                return Response.ok(String.valueOf(accu) + "\n").build();
            }

            final File[] processorPackageDirs = new File(serviceDir).listFiles((File file, String name) -> new File(file, name).isDirectory());
            StringBuilder accu = new StringBuilder("[");
            if (processorPackageDirs != null) {
                for (File processorPackageDir : processorPackageDirs) {
                    final String[] processorDescriptorFilenames = processorPackageDir.list((File _file, String name) -> name.endsWith("-descriptor.json"));
                    if (processorDescriptorFilenames != null) {
                        for (String filename : processorDescriptorFilenames) {
                            final String id = filename.substring(0, filename.length() - "-descriptor.json".length());
                            if (idMatcher.matches(processorPackageDir.getName() + "/" + id)) {
                                if (accu.length() > 1) {
                                    accu.append(", ");
                                }
                                accu.append("\"");
                                accu.append(processorPackageDir.getName());
                                accu.append("/");
                                accu.append(id);
                                accu.append("\"");
                            }
                        }
                    }
                }
            }
            if (accu.length() > 1) {
                accu.append("\n]");
            } else {
                accu.append("]");
            }

            return Response.ok(String.valueOf(accu) + "\n").build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }

    @GET
    @Path("/{package}/{name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response show(@PathParam("package") String pkg, @PathParam("name") String name, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String localPath = serviceDir + "/" + pkg + "/" + name + "-descriptor.json";
            final String maxAge = request.getHeader("max-age");

            // refresh from processor package
            if ("0".equals(maxAge) || ! new File(localPath).exists()) {
                final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
                final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
                CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(calvalusConfigPath);
                hadoopConnection.createJobClient(hadoopParameters);
                String content = hadoopConnection.getProcessorDescriptor(pkg + "/" + name, username);
                try (PrintStream out = new PrintStream(localPath)) {
                    out.print(content);
                }
                content = hadoopConnection.getExampleRequest(pkg + "/" + name, username);
                if (content != null) {
                    try (PrintStream out = new PrintStream(serviceDir + "/" + pkg + "/" + name + "-example-request.json")) {
                        out.print(content);
                    }
                }
                return Response.ok(content).build();
            }
            // read from cache
            if (new File(localPath).exists()) {
                final StringBuilder accu = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new FileReader(localPath))) {
                    String line;
                    while ((line = reader.readLine()) !=null){
                        accu.append(line);
                        accu.append("\n");
                    }
                }
                return Response.ok(String.valueOf(accu)).build();
            }
            return Response.notModified("processor descriptor  " + pkg + "/" + name + " not found").build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    private static CalvalusHadoopParameters readCalvalusConfiguration(File calvalusConfigPath) throws IOException {
        final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
        CalvalusHadoopParameters hadoopParameters = new CalvalusHadoopParameters();
        CalvalusHadoopRequestConverter.setHadoopDefaultParameters(hadoopParameters);
        for (Map.Entry<Object, Object> entry : calvalusConfig.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith("calvalus.hadoop.")) {
                key = key.substring("calvalus.hadoop.".length());
            }
            hadoopParameters.set(key, String.valueOf(entry.getValue()));
        }
        return hadoopParameters;
    }
}
