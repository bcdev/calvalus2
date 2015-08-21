package com.bc.calvalus.wps;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wps.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import org.junit.*;

import java.util.Map;

/**
 * Created by hans on 10/08/2015.
 */
public class CalvalusConfigTest {

    private static final String TOMCAT_DIRECTORY = "tomcatDirectory";

    private CalvalusDataInputs mockCalvalusDataInputs;

    private CalvalusConfig calvalusConfig;

    @Before
    public void setUp() throws Exception {
        mockCalvalusDataInputs = mock(CalvalusDataInputs.class);
        System.setProperty("catalina.base", TOMCAT_DIRECTORY);
    }

    @Test
    public void canGetExtractCustomConfig() throws Exception {
        when(mockCalvalusDataInputs.getValue("calvalus.calvalus.bundle")).thenReturn("calvalus-1.0-DUMMY");
        when(mockCalvalusDataInputs.getValue("calvalus.beam.bundle")).thenReturn("beam-1.0.0-DUMMY");
        when(mockCalvalusDataInputs.getValue("calvalus.wps.staging.path")).thenReturn("customStaging");

        calvalusConfig = new CalvalusConfig();
        Map<String, String> defaultConfig = calvalusConfig.getDefaultConfig(mockCalvalusDataInputs);

        assertThat(defaultConfig.get("production.db.type"), equalTo("memory"));
        assertThat(defaultConfig.get("calvalus.calvalus.bundle"), equalTo("calvalus-1.0-DUMMY"));
        assertThat(defaultConfig.get("calvalus.beam.bundle"), equalTo("beam-1.0.0-DUMMY"));
        assertThat(defaultConfig.get("calvalus.wps.staging.path"), equalTo("customStaging"));
    }

    @Test
    public void canGetDefaultConfigWhenCustomConfigsAreEmpty() throws Exception {
        when(mockCalvalusDataInputs.getValue("calvalus.calvalus.bundle")).thenReturn("");
        when(mockCalvalusDataInputs.getValue("calvalus.beam.bundle")).thenReturn("");
        when(mockCalvalusDataInputs.getValue("calvalus.wps.staging.path")).thenReturn("");

        calvalusConfig = new CalvalusConfig();
        Map<String, String> defaultConfig = calvalusConfig.getDefaultConfig(mockCalvalusDataInputs);

        assertThat(defaultConfig.get("production.db.type"), equalTo("memory"));
        assertThat(defaultConfig.get("calvalus.calvalus.bundle"), equalTo("calvalus-2.6-SNAPSHOT"));
        assertThat(defaultConfig.get("calvalus.beam.bundle"), equalTo("beam-5.0.1"));
        assertThat(defaultConfig.get("calvalus.wps.staging.path"), equalTo("staging"));
    }

    @Test
    public void canGetDefaultConfigWhenCustomConfigsAreNull() throws Exception {
        when(mockCalvalusDataInputs.getValue("calvalus.calvalus.bundle")).thenReturn(null);
        when(mockCalvalusDataInputs.getValue("calvalus.beam.bundle")).thenReturn(null);
        when(mockCalvalusDataInputs.getValue("calvalus.wps.staging.path")).thenReturn(null);

        calvalusConfig = new CalvalusConfig();
        Map<String, String> defaultConfig = calvalusConfig.getDefaultConfig(mockCalvalusDataInputs);

        assertThat(defaultConfig.get("production.db.type"), equalTo("memory"));
        assertThat(defaultConfig.get("calvalus.calvalus.bundle"), equalTo("calvalus-2.6-SNAPSHOT"));
        assertThat(defaultConfig.get("calvalus.beam.bundle"), equalTo("beam-5.0.1"));
        assertThat(defaultConfig.get("calvalus.wps.staging.path"), equalTo("staging"));
    }
}