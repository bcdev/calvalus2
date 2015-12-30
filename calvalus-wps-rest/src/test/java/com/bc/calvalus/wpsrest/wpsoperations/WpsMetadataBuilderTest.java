package com.bc.calvalus.wpsrest.wpsoperations;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import org.junit.*;

/**
 * @author hans
 */
public class WpsMetadataBuilderTest {

    @Test
    public void canCreateWpsMetadataWithAllParameters() throws Exception {
        ServletRequestWrapper mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        WpsMetadata wpsMetadata = WpsMetadataBuilder.create()
                    .withServletRequestWrapper(mockServletRequestWrapper)
                    .build();

        assertThat(wpsMetadata.getServletRequestWrapper(), equalTo(mockServletRequestWrapper));
    }

    @Test
    public void canCreateWpsMetadataWithNoParameters() throws Exception {
        WpsMetadata wpsMetadata = WpsMetadataBuilder.create().build();

        assertThat(wpsMetadata.getServletRequestWrapper(), equalTo(isNull()));
    }
}