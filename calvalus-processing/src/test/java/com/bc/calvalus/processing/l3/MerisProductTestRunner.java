/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.l3;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.File;

/**
 * A test runner that is used to run unit-level tests that require a MERIS data product
 * int the {@code testdata} directory.
 *
 * @author Marco Zuehlke
 */
public class MerisProductTestRunner extends BlockJUnit4ClassRunner {

    private static final String TESTDATA_DIR = "testdata";
    private static final String MERIS_PRODUCT = "MER_RR__1P_TEST.N1";

    public MerisProductTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        File testproductFile = getTestProductFile();
        if (testproductFile.exists()) {
            super.runChild(method, notifier);
        } else {
            System.err.println("Warning: test not performed: can't find " + testproductFile);
            notifier.fireTestIgnored(describeChild(method));
        }
    }

    static File getTestProductFile() {
        return new File(TESTDATA_DIR, MERIS_PRODUCT);
    }

}
