package com.bc.calvalus.wps.utils;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import org.junit.*;

/**
 * @author hans
 */
public class ProcessorNameConverterTest {

    private ProcessorNameConverter parser;

    @Test
    public void testParseValidIdentifier() throws Exception {
        String identifier = "beam-idepix~2.0.9~Idepix.Water";

        parser = new ProcessorNameConverter(identifier);

        assertThat(parser.getBundleName(), equalTo("beam-idepix"));
        assertThat(parser.getBundleVersion(), equalTo("2.0.9"));
        assertThat(parser.getExecutableName(), equalTo("Idepix.Water"));
    }

    @Test(expected = InvalidProcessorIdException.class)
    public void testParseIdentifierWithNoDelimiter() throws Exception {
        String identifier = "beam-idepix2.0.9Idepix.Water";

        parser = new ProcessorNameConverter(identifier);
    }

    @Test(expected = InvalidProcessorIdException.class)
    public void testParseIdentifierWithInvalidDelimiter() throws Exception {
        String identifier = "beam-idepix_2.0.9_Idepix.Water";

        parser = new ProcessorNameConverter(identifier);
    }

    @Test(expected = InvalidProcessorIdException.class)
    public void testParseIdentifierWithMissingProcessorName() throws Exception {
        String identifier = "beam-idepix~2.0.9";

        parser = new ProcessorNameConverter(identifier);
    }

    @Test(expected = InvalidProcessorIdException.class)
    public void testParseEmptyIdentifier() throws Exception {
        String identifier = "";

        parser = new ProcessorNameConverter(identifier);
    }

    @Test(expected = InvalidProcessorIdException.class)
    public void testParseNullIdentifier() throws Exception {
        parser = new ProcessorNameConverter(null);
    }

    @Test
    public void testConstructProcessorIdentifier() throws Exception {
        parser = new ProcessorNameConverter("DummyBundle", "0.0.1", "DummyExecutableName");

        assertThat(parser.getProcessorIdentifier(), equalTo("DummyBundle~0.0.1~DummyExecutableName"));
    }
}