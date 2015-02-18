package com.bc.calvalus.portal;

import org.apache.tools.ant.taskdefs.Javac;
import org.apache.tools.ant.types.Commandline;
import org.apache.tools.ant.types.Path;
import org.eclipse.jdt.core.JDTCompilerAdapter;

import java.io.File;

/**
 * This workaround is taken from:
 * http://code.google.com/p/google-web-toolkit/issues/detail?id=3557#c53
 * and need for the dev mode
 * should be fixed in GWT release after 2.5.1
 * In order to enable this set the following VM option:
 * -Dbuild.compiler=com.bc.calvalus.portal.JDTCompiler15
 */
public class JDTCompiler15 extends JDTCompilerAdapter {

    @Override
    public void setJavac(Javac javacTaskdef) {
        if (javacTaskdef.getTarget() == null) {
            javacTaskdef.setTarget("1.7"); //make target level to 1.6
        }
        if (javacTaskdef.getSource() == null) {
            javacTaskdef.setSource("1.7"); //make target level to 1.6
        }
        // redirect JSP compile output
        final Path srcPath = javacTaskdef.getSrcdir();
        if (srcPath != null) {
            String[] srcList = srcPath.list();
            if (srcList != null && srcList.length > 0) {
                javacTaskdef.setDestdir(new File(srcList[0]));
            }
        }
        super.setJavac(javacTaskdef);
    }

    // This method is responsible for printing the errors/warning.
    @Override
    protected void logAndAddFilesToCompile(Commandline cmd) {
        super.logAndAddFilesToCompile(cmd);
        System.err.println(cmd.toString());
    }
}