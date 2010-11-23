package com.bc.calvalus.experiments.executables;

import com.bc.calvalus.experiments.util.CalvalusLogger;

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

    public ExecutablesInstaller(String archiveRootPath, String installationRootPath) {
        this.archiveRootPath = archiveRootPath;
        this.installationRootPath = installationRootPath;
    }

    /** Checks whether package version is installed, else installs it using archived install script */
    public File maybeInstallProcessorPackage(String packageName, String packageVersion)
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
    public File maybeInstallCallXsl(String packageName,
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
