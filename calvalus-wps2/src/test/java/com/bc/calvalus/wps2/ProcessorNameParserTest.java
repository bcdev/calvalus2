package com.bc.calvalus.wps2;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

import org.junit.*;

/**
 * Created by hans on 14/08/2015.
 */
public class ProcessorNameParserTest {

    /**
     * Class under test.
     */
    private ProcessorNameParser parser;

    @Test
    public void testParseValidIdentifier() throws Exception {
        String identifier = "beam-idepix~2.0.9~Idepix.Water";

        parser = new ProcessorNameParser(identifier);

        assertThat(parser.getBundleName(), equalTo("beam-idepix"));
        assertThat(parser.getBundleVersion(), equalTo("2.0.9"));
        assertThat(parser.getExecutableName(), equalTo("Idepix.Water"));
    }

    @Test(expected = WpsException.class)
    public void testParseIdentifierWithNoDelimiter() throws Exception {
        String identifier = "beam-idepix2.0.9Idepix.Water";

        parser = new ProcessorNameParser(identifier);
    }

    @Test(expected = WpsException.class)
    public void testParseIdentifierWithInvalidDelimiter() throws Exception {
        String identifier = "beam-idepix_2.0.9_Idepix.Water";

        parser = new ProcessorNameParser(identifier);
    }

    @Test(expected = WpsException.class)
    public void testParseIdentifierWithMissingName() throws Exception {
        String identifier = "beam-idepix~2.0.9";

        parser = new ProcessorNameParser(identifier);
    }

    @Test(expected = WpsException.class)
    public void testParseEmptyIdentifier() throws Exception {
        String identifier = "";

        parser = new ProcessorNameParser(identifier);
    }

    @Test(expected = WpsException.class)
    public void testParseNullIdentifier() throws Exception {
        parser = new ProcessorNameParser(null);
    }
}