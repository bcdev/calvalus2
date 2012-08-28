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
@Deprecated
public class ExecutablesInstaller {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String DEFAULT_SCRIPT_FILENAME = "calvalus-call.xsl";
    private static final String DEFAULT_INSTALLER_FILENAME = "calvalus-install.sh";

    final String archiveRootPath;
    final String installationRootPath;
    final Mapper.Context context;

    public ExecutablesInstaller(Mapper.Context context, String archiveRootPath, String installationRootPath) {
        this.archiveRootPath = archiveRootPath;
        this.installationRootPath = installationRootPath;
        this.context = context;
    }

    /** Checks whether package version is installed and up-to-date, else installs it using install script */
    public File maybeInstallProcessorPackage(String packageName, String packageVersion)
        throws IOException, InterruptedException
    {
        // check package availability in software archive
        final String packageFilename = packageName + "-" + packageVersion + ".tar.gz";
        Path archivePackage = new Path(archiveRootPath, packageFilename);
        FileSystem fs = archivePackage.getFileSystem(context.getConfiguration());
        if (! fs.exists(archivePackage)) {
            throw new ProcessorException(archivePackage.toUri().getPath() + " installation package not found");
        }
        // check package dir availability and age in installation dir
        File installationRootDir = new File(installationRootPath);
        File packageDir          = new File(installationRootDir, packageName + "-" + packageVersion);
        if (packageDir.exists() && packageDir.lastModified() >= fs.listStatus(archivePackage)[0].getModificationTime()) {
            LOG.info("package " + packageName + "-" + packageVersion + " up-to-date");
            return packageDir;
        }

        // determine package installation script
        String installationScriptFilename = packageName + "-" + packageVersion + "-install.sh";
        Path packageInstallScript = new Path(archiveRootPath, installationScriptFilename);
        if (!fs.exists(packageInstallScript)) {
            installationScriptFilename = DEFAULT_INSTALLER_FILENAME;
            packageInstallScript = new Path(archiveRootPath, installationScriptFilename);
            if (!fs.exists(packageInstallScript))
                throw new ProcessorException(packageInstallScript.toUri().getPath() + " install script not found");
        }

        // install package from software archive
        installationRootDir.mkdirs();
        fs.copyToLocalFile(archivePackage, new Path(installationRootPath + "/" + packageFilename));
        fs.copyToLocalFile(packageInstallScript, new Path(installationRootPath + "/" + installationScriptFilename));
        ProcessUtil installation = new ProcessUtil();
        installation.directory(installationRootDir);
        if (installation.run("/bin/bash",
                             "-c",
                             ". " + installationRootPath + "/" + installationScriptFilename + " " +
                                     installationRootPath + "/" + packageFilename + " " +
                                     installationRootPath + " " +
                                     packageName + "-" + packageVersion) != 0) {
            throw new ProcessorException("package installation with " + installationScriptFilename + " failed: " + installation.getOutputString());
        }

        LOG.info("package " + packageName + "-" + packageVersion + " installed with " + installationScriptFilename + ": " + installation.getOutputString());
        return packageDir;
    }

    /** Checks whether XSL or executable script is installed, else copies it into package installation.
     * Three cases:
     * <ul><li>script and tgz</li><li>xsl and tgz</li><li>script or xsl only</li></ul>
     */
    public File maybeInstallScripts(String packageName,
                                    String packageVersion,
                                    String requestType,
                                    File packageDir)
        throws IOException, InterruptedException
    {
        String xslScriptFilename = packageName + "-" + packageVersion + "-" + requestType + "-call.xsl";
        String bashScriptFilename = packageName + "-" + packageVersion + "-" + requestType + "-call.bash";
        String scriptFilename = requestType;
        Path archiveDefaultScript = new Path(archiveRootPath, DEFAULT_SCRIPT_FILENAME);
        Path archiveXslScript = new Path(archiveRootPath, xslScriptFilename);
        Path archiveBashScript = new Path(archiveRootPath, bashScriptFilename);
        //Path archiveScriptWithPackage = new Path(new Path(archiveRootPath,String.valueOf(packageName)).getParent(),scriptFilename);
        Path archiveScript = new Path(archiveRootPath, scriptFilename);
        FileSystem fs = archiveDefaultScript.getFileSystem(context.getConfiguration());
        File installationRootDir = new File(installationRootPath);
        File script = null;

        // check and maybe install default xsl script
        if (! fs.exists(archiveDefaultScript)) {
            throw new ProcessorException(archiveDefaultScript.toUri().getPath() + " XSL script not found");
        }
        File defaultScript = new File(installationRootDir, DEFAULT_SCRIPT_FILENAME);
        if (! defaultScript.exists() || defaultScript.lastModified() < fs.listStatus(archiveDefaultScript)[0].getModificationTime()) {
            fs.copyToLocalFile(archiveDefaultScript, new Path(defaultScript.getPath()));
        }

        if (packageName != null && packageVersion != null && fs.exists(archiveXslScript)) {
            // xsl+tgz
            script = new File(packageDir, xslScriptFilename);
            maybeInstallScript(xslScriptFilename, archiveXslScript, script, fs);
        } else if (packageName != null && packageVersion != null && fs.exists(archiveBashScript)) {
            // sh+tgz
            script = new File(installationRootDir, bashScriptFilename);
            maybeInstallScript(bashScriptFilename, archiveBashScript, script, fs);
        } else if ((packageName == null || packageVersion != null) && fs.exists(archiveScript)) {
            // sh but no tgz
            script = new File(installationRootDir, requestType);
            maybeInstallScript(requestType, archiveScript, script, fs);
        } else {
            // tgz with sh or xsl in tgz root dir
            script = new File(packageDir, requestType);
            if (! script.exists()) {
                throw new ProcessorException("file " + script.getPath() + " not found");
            }
        }
        return script;
    }

    /** Checks whether script exists and is up to date, else copies it from archive */
    private void maybeInstallScript(String xslScriptFilename, Path archiveXslScript, File script, FileSystem fs) throws IOException {
        if (script.exists() && script.lastModified() >= fs.listStatus(archiveXslScript)[0].getModificationTime()) {
            LOG.info("XSL script " + xslScriptFilename + " up-to-date");
        } else {
            fs.copyToLocalFile(archiveXslScript, new Path(script.getPath()));
            LOG.info("XSL script " + xslScriptFilename + " installed");
        }
    }
}
