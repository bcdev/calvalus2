package com.bc.calvalus.commons.util;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

import org.junit.*;

/**
 * @author hans
 */
public class PropertiesWrapperTest {

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("test.properties");
    }

    @Test
    public void canGetStringValue() throws Exception {
        assertThat(PropertiesWrapper.get("property1"), equalTo("test"));
        assertThat(PropertiesWrapper.get("property2"), equalTo("test2"));
    }

    @Test
    public void canGetIntegerValue() throws Exception {
        assertThat(PropertiesWrapper.getInteger("property3"), equalTo(100));
        assertThat(PropertiesWrapper.getInteger("property4"), equalTo(905));
    }

    @Test
    public void canGetIntegerValueAsDouble() throws Exception {
        assertThat(PropertiesWrapper.getDouble("property3"), equalTo(100.0));
        assertThat(PropertiesWrapper.getDouble("property4"), equalTo(905.0));
    }

    @Test
    public void canGetDoubleValue() throws Exception {
        assertThat(PropertiesWrapper.getDouble("property5"), equalTo(100.5));
        assertThat(PropertiesWrapper.getDouble("property6"), equalTo(905.99));
    }

    @Test
    public void canReturnNullWhenKeyNotAvailable() throws Exception {
        assertThat(PropertiesWrapper.get("property100"), equalTo(null));
    }

    @Test
    public void canReturnEmptyStringWhenValueIsEmpty() throws Exception {
        assertThat(PropertiesWrapper.get("property7"), equalTo(""));
    }

    @Test(expected = NumberFormatException.class)
    public void canThrowExceptionWhenFormatMismatch() throws Exception {
        PropertiesWrapper.getInteger("property1");
    }

    @Test(expected = NumberFormatException.class)
    public void canThrowExceptionWhenFormatMismatch2() throws Exception {
        PropertiesWrapper.getInteger("property5");
    }

}