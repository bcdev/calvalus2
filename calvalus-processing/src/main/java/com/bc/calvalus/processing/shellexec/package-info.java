/**
 * Provides a generic adapter that makes it possible to run Unix shell executables in Hadoop MapReduce.
 * The current implementation uses XSLT to generate any (shell or script) code required to
 * run the executable using the parametrization (input, output, processing parameters) provided by the
 * Calvalus environment.
 *
 * @since 1.0
 * @author Martin Boettcher
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
package com.bc.calvalus.processing.shellexec;