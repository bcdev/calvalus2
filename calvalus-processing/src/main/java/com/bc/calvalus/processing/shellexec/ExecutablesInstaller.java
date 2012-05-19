package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Checks and maybe installs processor package and processor call transformation
 *
 * @author Martin Boettcher
 */
public class ExecutablesInstaller {

    private static final Logger LOG = CalvalusLogger.getLogger();

    final String archiveRootPath;
    final String installationRootPath;
    final Mapper.Context context;

    public ExecutablesInstaller(Mapper.Context context, String archiveRootPath, String installationRootPath) {
        this.archiveRootPath = archiveRootPath;
        this.installationRootPath = installationRootPath;
        this.context = context;
    }

    /** Checks whether package version is installed, else installs it using archived install script */
    public File maybeInstallProcessorPackage(String packageName, String packageVersion)
        throws IOException, InterruptedException
    {
        String installationScriptFilename = packageName + "-" + packageVersion + "-install.sh";
        // check package availability in software archive
        Path archivePackage = new Path(archiveRootPath, packageName + "-" + packageVersion + ".tar.gz");
        FileSystem fs = archivePackage.getFileSystem(context.getConfiguration());
        if (! fs.exists(archivePackage))
            throw new ProcessorException(archivePackage.toUri().getPath() + " installation package not found");
        // check package dir availability and age in installation dir
        File installationRootDir = new File(installationRootPath);
        File packageDir          = new File(installationRootDir, packageName + "-" + packageVersion);
        if (! packageDir.exists() || packageDir.lastModified() < fs.listStatus(archivePackage)[0].getModificationTime()) {
            LOG.info("installation " + installationScriptFilename + " ...");
            // check package installation script availability in software archive
            Path packageInstallScript = new Path(archiveRootPath, installationScriptFilename);
            if (! fs.exists(packageInstallScript))
                throw new ProcessorException(packageInstallScript.toUri().getPath() + " install script not found");
            // install package from software archive
            installationRootDir.mkdirs();
            fs.copyToLocalFile(archivePackage, new Path(installationRootPath + "/" + packageName + "-" + packageVersion + ".tar.gz"));
            fs.copyToLocalFile(packageInstallScript, new Path(installationRootPath + "/" + installationScriptFilename));
            ProcessUtil installation = new ProcessUtil();
            installation.directory(installationRootDir);
            if (installation.run("/bin/bash",
                                 "-c",
                                 ". " + installationRootPath + "/" + installationScriptFilename + " " +
                                 installationRootPath + "/" + packageName + "-" + packageVersion + ".tar.gz" + " " +
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
    public File maybeInstallCallXsl(String packageName,
                                    String packageVersion,
                                    String requestType)
        throws IOException, InterruptedException
    {
        String callXslFilename   = packageName + "-" + packageVersion + "-" + requestType + "-call.xsl";
        // check transformation file availability in software archive
        Path archiveCallXsl = new Path(archiveRootPath, callXslFilename);
        FileSystem fs = archiveCallXsl.getFileSystem(context.getConfiguration());
        if (! fs.exists(archiveCallXsl)) throw new ProcessorException(archiveCallXsl.toUri().getPath() + " call transformation not found");
        // -- check transformation file availability and age in package directory
        File installationRootDir = new File(installationRootPath);
        File packageDir          = new File(installationRootDir, packageName + "-" + packageVersion);
        File callXsl             = new File(packageDir, callXslFilename);
        if (! callXsl.exists() || callXsl.lastModified() < fs.listStatus(archiveCallXsl)[0].getModificationTime()) {
            LOG.info("installation of " + callXslFilename + " ...");
            // copy transformation file into package directory
            fs.copyToLocalFile(archiveCallXsl, new Path(callXsl.getPath()));
        }
        return callXsl;
    }
}
