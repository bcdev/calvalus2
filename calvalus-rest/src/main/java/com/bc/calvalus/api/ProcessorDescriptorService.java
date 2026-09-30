package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.sun.jersey.api.NotFoundException;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.sun.jersey.api.client.ClientResponse;

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
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("processor-descriptors")
public class ProcessorDescriptorService {

    private static Logger LOG = CalvalusLogger.getLogger();

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    public Response list(
            @QueryParam("names") String names,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) {
        try {
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String maxAge = request.getHeader("max-age");
            final DescriptorUtils.IdMatcher idMatcher = new DescriptorUtils.IdMatcher(names);
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);

            if ("0".equals(maxAge)) {
                final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
                final CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(calvalusConfigPath);
                final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
                hadoopConnection.createJobClient(hadoopParameters);
                final StringBuilder accu = new StringBuilder("[");
                stageProcessorDescriptors(hadoopConnection, username, serviceDir, idMatcher, roleMatcher, accu);
                stageProcessorDescriptors(hadoopConnection, null, serviceDir, idMatcher, roleMatcher, accu);
                accu.append("]");
                return Response.ok(String.valueOf(accu) + "\n").build();
            }

            final StringBuilder accu = new StringBuilder("[");
            final File[] processorPackageDirs = new File(serviceDir).listFiles(
                    (File file, String name) -> new File(file, name).isDirectory()
            );
            if (processorPackageDirs != null) {
                for (File processorPackageDir : processorPackageDirs) {
                    final String[] processorDescriptorFilenames = processorPackageDir.list(
                            (File _file, String name) -> name.endsWith("-descriptor.json")
                    );
                    if (processorDescriptorFilenames != null) {
                        for (String filename : processorDescriptorFilenames) {
                            final String processorName = filename.substring(
                                    0,
                                    filename.length() - "-descriptor.json".length()
                            );

                            if (idMatcher.matches(processorPackageDir.getName() + "/" + processorName) &&
                                roleMatcher.matches(processorPackageDir, filename))
                            {
                                if (accu.length() > 1) {
                                    accu.append(", ");
                                }
                                accu.append("\"");
                                accu.append(processorPackageDir.getName());
                                accu.append("/");
                                accu.append(processorName);
                                accu.append("\"");
                            }
                        }
                    }
                }
            }
            accu.append("]");

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
    @Produces({ MediaType.APPLICATION_JSON })
    public Response show(@PathParam("package") String pkg, @PathParam("name") String name, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String localPath = serviceDir + "/" + pkg + "/" + name + "-descriptor.json";
            final String maxAge = request.getHeader("max-age");
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);

            // refresh from processor package
            if ("0".equals(maxAge) || ! new File(localPath).exists()) {
                final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
                final CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(calvalusConfigPath);
                final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
                hadoopConnection.createJobClient(hadoopParameters);
                final int pos = pkg.indexOf('@');
                final String packagePath = pos == -1
                        ? hadoopConnection.getProcessorRootDir(pkg.substring(pos + 1)) + "/" + pkg.substring(0, pos)
                        : hadoopConnection.getProcessorRootDir(null) + "/" + pkg;
                final String content = hadoopConnection.readFile(packagePath + "/" + name + "-descriptor.json");
                if (! roleMatcher.matches(content)) {
                    return Response
                            .status(ClientResponse.Status.NOT_FOUND)
                            .entity("processor descriptor  " + pkg + "/" + name + " not found").build();
                }
                try (PrintStream out = new PrintStream(localPath)) {
                    out.print(content);
                }
                // copy example files
                for (String exampleFilename : hadoopConnection.listFiles(
                        packagePath,
                        name,
                        "-example-request.json")) {
                    final String exampleContent = hadoopConnection.readFile(packagePath + "/" + exampleFilename);
                    final String localExamplePath = serviceDir + "/" + pkg + "/" + exampleFilename;
                    try (PrintStream out = new PrintStream(localExamplePath)) {
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
                String content = String.valueOf(accu);
                if (roleMatcher.matches(content)) {
                    return Response.ok(content).build();
                }
            }
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity("processor descriptor  " + pkg + "/" + name + " not found")
                    .build();
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

    private void stageProcessorDescriptors(
            CalvalusHadoopConnection hadoopConnection,
            String userName,
            String serviceDir,
            DescriptorUtils.IdMatcher idMatcher,
            DescriptorUtils.RoleMatcher roleMatcher,
            StringBuilder accu
    ) throws IOException {
        String processorRootDir = hadoopConnection.getProcessorRootDir(userName);
        if (hadoopConnection.exists(processorRootDir)) {
            LOG.info("scanning " + processorRootDir);
            for (String processorPackageName : hadoopConnection.listSubdirs(processorRootDir)) {
                LOG.info("scanning package " + processorPackageName);
                for (String descriptorFilename : hadoopConnection.listFiles(
                        processorRootDir + "/" + processorPackageName,
                        "",
                        "-descriptor.json"
                )) {
                    String processorName = descriptorFilename.substring(
                            0,
                            descriptorFilename.length() + "-descriptor.json".length()
                    );
                    final String packageId = userName == null
                            ? processorPackageName
                            : processorPackageName + "@" + userName;
                    LOG.info("checking " + processorName);
                    if (idMatcher.matches(packageId + "/" + processorName)) {
                        final String descriptorPath = processorRootDir + "/" + processorPackageName + "/" + descriptorFilename;
                        final String content = hadoopConnection.readFile(descriptorPath);
                        if (roleMatcher.matches(content)) {
                            // copy descriptor
                            final String localPath = serviceDir + "/" + packageId + "/" + descriptorFilename;
                            new File(localPath).getParentFile().mkdirs();
                            try (PrintStream out = new PrintStream(localPath)) {
                                out.print(content);
                            }
                            // copy example requests
                            for (String exampleFilename : hadoopConnection.listFiles(
                                processorRootDir + "/" + processorPackageName,
                                processorName,
                                "-example-request.json"
                            )) {
                                final String examplePath = processorRootDir + "/" + processorPackageName + "/" + exampleFilename;
                                final String exampleContent = hadoopConnection.readFile(examplePath);
                                final String localExamplePath = serviceDir + "/" + packageId + "/" + exampleFilename;
                                try (PrintStream out = new PrintStream(localExamplePath)) {
                                    out.print(exampleContent);
                                }
                            }
                            // add processor to result
                            if (accu.length() > 1) {
                                accu.append(", ");
                            }
                            accu.append("\"");
                            accu.append(processorPackageName);
                            accu.append("/");
                            accu.append(processorName);
                            accu.append("\"");
                            LOG.info("staged " + processorPackageName + "/" + processorName);
                        }
                    }
                }
            }
        }
    }

}
