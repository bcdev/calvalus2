package com.bc.calvalus.portal.server;

import org.jasig.cas.client.validation.AbstractTicketValidationFilter;
import org.jasig.cas.client.validation.TicketValidator;

import javax.servlet.FilterConfig;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SamlCreateTicketValidationFilter extends AbstractTicketValidationFilter {

    private static final Set<String> RESERVED_INIT_PARAMS = new HashSet<>(Arrays.asList( "casServerUrlPrefix", "renew",
	    "exceptionOnValidationFailure", "redirectAfterValidation", "useSession", "serverName", "service",
	    "artifactParameterName", "serviceParameterName", "encodeServiceUrl", "hostnameVerifier", "encoding",
	    "config" ));

    /**
     * Constructs a SamlCreateTicketValidator.
     *
     * @param filterConfig  filter configuration
     * @return the TicketValidator.
     */
    protected final TicketValidator getTicketValidator(final FilterConfig filterConfig) {
        final String casServerUrlPrefix = getPropertyFromInitParams(filterConfig, "casServerUrlPrefix", null);
        final String calvalusPrivateKeyPath = getPropertyFromInitParams(filterConfig, "calvalus.crypt.samlkey-private-key", null);
        if (calvalusPrivateKeyPath == null) {
            throw new RuntimeException("Must configure path to calvalus private key, property 'calvalus.crypt.samlkey-private-key'");
        }
        final SamlCreateTicketValidator validator = new SamlCreateTicketValidator(casServerUrlPrefix, calvalusPrivateKeyPath);

        validator.setRenew(parseBoolean(getPropertyFromInitParams(filterConfig, "renew", "false")));
        validator.setEncoding(getPropertyFromInitParams(filterConfig, "encoding", null));

        final Map<String,String> additionalParameters = new HashMap<String,String>();

        for (final Enumeration<?> e = filterConfig.getInitParameterNames(); e.hasMoreElements();) {
            final String s = (String) e.nextElement();

            if (!RESERVED_INIT_PARAMS.contains(s)) {
                additionalParameters.put(s, filterConfig.getInitParameter(s));
            }
        }

        validator.setCustomParameters(additionalParameters);
        validator.setHostnameVerifier(getHostnameVerifier(filterConfig));

        return validator;
    }
}
