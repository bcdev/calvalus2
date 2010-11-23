package com.bc.calvalus.experiments.executables;

import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor adapter for executables.
 * <ul>
 * <li>Checks and maybe installs processor</li>
 * <li>transforms request to command line call string and optionally parameter file(s)</li>
 * <li>calls executable</li>
 * <li>handles return code and stderr/stdout</li>
 * </ul>
 *
 * @author Boe
 */
public class ExecutablesMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String TYPE_XPATH = "/wps:Execute/ows:Identifier";
    //private static final String OUTPUT_DIR_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:LiteralData";
    //private static final String PACKAGE_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.processor.package']/wps:Data/wps:LiteralData";
    //private static final String VERSION_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.processor.version']/wps:Data/wps:LiteralData";
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
    private static final String PACKAGE_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.package']/Data/LiteralData";
    private static final String VERSION_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.version']/Data/LiteralData";

    /**
     * Mapper implementation method. See class comment.
     * @param context  task "configuration"
     * @throws IOException  if installation or process initiation raises it
     * @throws InterruptedException   if processing is interrupted externally
     * @throws ProcessorException  if processing fails
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {

        try {
            final FileSplit split = (FileSplit) context.getInputSplit();

            // parse request
            final String requestContent = context.getConfiguration().get("calvalus.request");
            final XmlDoc request = new XmlDoc(requestContent);
            final String requestType = request.getString(TYPE_XPATH);
            final String requestOutputPath = request.getString(OUTPUT_DIR_XPATH);
            final String packageName = request.getString(PACKAGE_XPATH);
            final String packageVersion = request.getString(VERSION_XPATH);

            // TODO move constants to some configuration
            final String installationRootPath = "/home/hadoop/opt";
            final String archiveMountPath = "/mnt/hdfs";
            final String archiveRootPath = archiveMountPath + "/calvalus/software/0.5";

            // check for and maybe install processor package and XSL for request type
            final ExecutablesInstaller installer =
                new ExecutablesInstaller(archiveRootPath, installationRootPath);
            final File packageDir =
                installer.maybeInstallProcessorPackage(packageName, packageVersion);
            final File callXsl =
                installer.maybeInstallCallXsl(packageName, packageVersion, requestType);

            // transform request into command line, may write parameter files as side effect
            final XslTransformer xslt = new XslTransformer(new File(callXsl.getPath()));
            //xslt.setParameter("calvalus.input", archiveMountPath + File.separator + split.getPath().toUri().getPath());
            xslt.setParameter("calvalus.input", split.getPath().toUri());
            xslt.setParameter("calvalus.task.id", context.getTaskAttemptID());
            xslt.setParameter("calvalus.package.dir", packageDir.getPath());
            xslt.setParameter("calvalus.archive.mount", archiveMountPath);
            final String commandLine = xslt.transform(request.getDocument());
            LOG.info("command line to be executed: " + commandLine);

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            final long startTime = System.nanoTime();

            // ensure that output dir exists
            final String requestOutputPhysicalPath = (requestOutputPath.startsWith("hdfs:"))
                    ? archiveMountPath + File.separator + new Path(requestOutputPath).toUri().getPath()
                    : new Path(requestOutputPath).toUri().getPath();
            final File requestOutputDir = new File(requestOutputPhysicalPath);
            requestOutputDir.mkdirs();

            // run process for command line
            final Context theContext = context;
            final ProcessUtil processor = new ProcessUtil(new ProcessUtil.OutputObserver() {
                public void handle(String line) {
                    LOG.info(line);    // TODO take care for verbose processors
                    theContext.progress();
                }
            });
            processor.directory(requestOutputDir);
            final int returnCode = processor.run("/bin/bash", "-c", commandLine);
            if (returnCode == 0) {
                LOG.info("execution of " + commandLine + " successful: " + processor.getOutputString());
            } else {
                throw new ProcessorException("execution of " + commandLine + " failed: " + processor.getOutputString());
            }

            // write final log entry for runtime measurements
            final long stopTime = System.nanoTime();
            LOG.info(context.getTaskAttemptID() + " stops processing of split " + split + " after " + ((stopTime - startTime) / 1E9) + " sec");

        } catch (ProcessorException e) {
            LOG.warning(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "ExecutablesMapper exception: " + e.toString(), e);
            throw new ProcessorException("ExecutablesMapper exception: " + e.toString(), e);
        }
    }

    /** Checks whether package version is installed, else installs it using archived install script */
    private File maybeInstallProcessorPackage(String archiveRootPath,
                                              String installationRootPath,
                                              String packageName,
                                              String packageVersion)
        throws IOException, InterruptedException
    {
        String installationScriptFilename = packageName + "-" + packageVersion + "-install.sh";
        // check package availability in software archive
        File archivePackage = new File(archiveRootPath, packageName + "-" + packageVersion + ".tar.gz");  // TODO generalise to allow zip
        if (!archivePackage.exists())
            throw new ProcessorException(archivePackage.getPath() + " installation package not found");
        // check package dir availability and age in installation dir
        File installationRootDir = new File(installationRootPath);
        File packageDir          = new File(installationRootDir, packageName + "-" + packageVersion);
        if (! packageDir.exists() || packageDir.lastModified() < archivePackage.lastModified()) {
            LOG.info("installation " + installationScriptFilename + " ...");
            // check package installation script availability in software archive
            File packageInstallScript = new File(archiveRootPath, installationScriptFilename);
            if (! packageInstallScript.exists())
                throw new ProcessorException(packageInstallScript.getPath() + " install script not found");
            // install package from software archive
            installationRootDir.mkdirs();
            ProcessUtil installation = new ProcessUtil();
            installation.directory(installationRootDir);
            if (installation.run("/bin/bash",  // hdfs does not support exe mode of files
                                 "-c",
                                 ". " + packageInstallScript.getPath() + " " +
                                 archivePackage.getPath() + " " +
                                 installationRootPath + " " +
                                 packageName + "-" + packageVersion) == 0) {
                LOG.info("installation " + installationScriptFilename + " successful: " + installation.getOutputString());
            } else {
                throw new ProcessorException("installation " + installationScriptFilename + " failed: " + installation.getOutputString());
            }
        }
        return packageDir;
    }

    /** Checks whether request type call XSL is installed, else copies it into package installation */
    private File maybeInstallCallXsl(String archiveRootPath,
                                     String installationRootPath,
                                     String packageName,
                                     String packageVersion,
                                     String requestType)
        throws IOException, InterruptedException
    {
        String callXslFilename   = packageName + "-" + packageVersion + "-" + requestType + "-call.xsl";
        // check transformation file availability in software archive
        File archiveCallXsl = new File(archiveRootPath, callXslFilename);
        if (! archiveCallXsl.exists()) throw new ProcessorException(archiveCallXsl.getPath() + " call transformation not found");
        // -- check transformation file availability and age in package directory
        File installationRootDir = new File(installationRootPath);
        File packageDir          = new File(installationRootDir, packageName + "-" + packageVersion);
        File callXsl             = new File(packageDir, callXslFilename); 
        if (! callXsl.exists() || callXsl.lastModified() < archiveCallXsl.lastModified()) {
            LOG.info("installation of " + callXslFilename + " ...");
            // copy transformation file into package directory
            ProcessUtil installation = new ProcessUtil();
            installation.directory(installationRootDir);
            if (installation.run("/bin/cp", archiveCallXsl.getPath(), callXsl.getPath()) == 0) {
                LOG.info("installation of " + callXslFilename + " successful: " + installation.getOutputString());
            } else {
                throw new ProcessorException("installation of " + callXslFilename + " failed: " + installation.getOutputString());
            }
        }
        return callXsl;
    }
}
