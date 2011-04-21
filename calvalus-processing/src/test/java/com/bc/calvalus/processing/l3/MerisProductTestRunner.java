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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * A test runner that is used to run unit-level tests that require a MERIS data product
 * int the {@code testdata} directory.
 *
 * @author Marco Zuehlke
 */
public class MerisProductTestRunner extends BlockJUnit4ClassRunner {

    static final String TEST_DATA = "/eodata/MER_RR__1P_TEST.N1";

    public MerisProductTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        File file = getTestProductFile();
        if (file.exists()) {
            super.runChild(method, notifier);
        } else {
            System.err.println("Warning: test not performed: can't find " + file);
            notifier.fireTestIgnored(describeChild(method));
        }
    }

    static File getTestProductFile() {
        final URL resource = MerisProductTestRunner.class.getResource(TEST_DATA);
        if (resource == null) {
            throw new IllegalStateException("Resource not found: " + TEST_DATA);
        }
        final URI uri;
        try {
            uri = resource.toURI();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        return new File(uri);
    }

}
