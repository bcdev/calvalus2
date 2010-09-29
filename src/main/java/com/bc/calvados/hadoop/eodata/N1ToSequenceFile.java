package com.bc.calvados.hadoop.eodata;

import com.bc.childgen.ChildGenException;
import com.bc.childgen.ChildGeneratorFactory;
import com.bc.childgen.ChildGeneratorImpl;

public class N1ToSequenceFile {
    public static void main(String[] args) {
        String inputFileName = args[0];
        ChildGeneratorImpl childGenerator = null;
        try {
            childGenerator = ChildGeneratorFactory.createChildGenerator(inputFileName);
        } catch (ChildGenException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }

        int numLinesTotal = 0;
        int firstLine = 0;
        int lastLine = 0;
        /*
        childGenerator.process(new FSImageInputStream(),
                               new FsImageOutputStream(),
                               firstLine, lastLine);
                               */

    }
}
