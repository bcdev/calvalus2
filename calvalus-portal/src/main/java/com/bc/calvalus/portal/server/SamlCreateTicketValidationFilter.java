package com.bc.calvalus.portal.server;

import static com.bc.calvalus.portal.server.BCAuthenticationFilter.INJECT_INPUT_SELECTION_PREFIX;

import org.jasig.cas.client.util.CommonUtils;
import org.jasig.cas.client.validation.AbstractTicketValidationFilter;
import org.jasig.cas.client.validation.Assertion;
import org.jasig.cas.client.validation.TicketValidator;

import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SamlCreateTicketValidationFilter extends AbstractTicketValidationFilter {

    private static final Set<String> RESERVED_INIT_PARAMS = new HashSet<>(Arrays.asList("casServerUrlPrefix", "renew",
                                                                                        "exceptionOnValidationFailure",
                                                                                        "redirectAfterValidation",
                                                                                        "useSession", "serverName",
                                                                                        "service",
                                                                                        "artifactParameterName",
                                                                                        "serviceParameterName",
                                                                                        "encodeServiceUrl",
                                                                                        "hostnameVerifier", "encoding",
                                                                                        "config"));
    private static final String INJECT_INPUT_SELECTION_SERVLET = "https://processing.code-de-ref.eoc.dlr.de/injectInputSelection";
    private static final String PROCESSING_SERVER_NAME = "https://processing.code-de-ref.eoc.dlr.de";

    /**
     * Constructs a SamlCreateTicketValidator.
     *
     * @param filterConfig filter configuration
     *
     * @return the TicketValidator.
     */
    protected final TicketValidator getTicketValidator(final FilterConfig filterConfig) {
        final String casServerUrlPrefix = getPropertyFromInitParams(filterConfig, "casServerUrlPrefix", null);
        final String calvalusPrivateKeyPath = getPropertyFromInitParams(filterConfig,
                                                                        "calvalus.crypt.samlkey-private-key", null);
        if (calvalusPrivateKeyPath == null) {
            throw new RuntimeException(
                        "Must configure path to calvalus private key, property 'calvalus.crypt.samlkey-private-key'");
        }
        final SamlCreateTicketValidator validator = new SamlCreateTicketValidator(casServerUrlPrefix,
                                                                                  calvalusPrivateKeyPath);

        validator.setRenew(parseBoolean(getPropertyFromInitParams(filterConfig, "renew", "false")));
        validator.setEncoding(getPropertyFromInitParams(filterConfig, "encoding", null));

        final Map<String, String> additionalParameters = new HashMap<>();

        for (final Enumeration<?> e = filterConfig.getInitParameterNames(); e.hasMoreElements(); ) {
            final String s = (String) e.nextElement();

            if (!RESERVED_INIT_PARAMS.contains(s)) {
                additionalParameters.put(s, filterConfig.getInitParameter(s));
            }
        }

        validator.setCustomParameters(additionalParameters);
        validator.setHostnameVerifier(getHostnameVerifier(filterConfig));

        return validator;
    }

    @Override
    protected void onSuccessfulValidation(HttpServletRequest request, HttpServletResponse response,
                                          Assertion assertion) {
        boolean injectInputSelection = false;
        HttpSession session = request.getSession();
        String sessionId = "";
        if (session != null) {
            sessionId = session.getId();
            Object attributeValue = session.getAttribute(INJECT_INPUT_SELECTION_PREFIX + sessionId);
            if (attributeValue != null) {
                injectInputSelection = (Boolean) attributeValue;
            }
        }
        if (injectInputSelection) {
            String redirectUrl = CommonUtils.constructServiceUrl(request,
                                                                 response,
                                                                 INJECT_INPUT_SELECTION_SERVLET,
                                                                 PROCESSING_SERVER_NAME,
                                                                 "ticket",
                                                                 true);
            try {
                System.out.println("Redirect URL : " + redirectUrl);
                response.sendRedirect(redirectUrl);
            } catch (IOException e) {
                System.err.println("Unable to redirect to " + redirectUrl);
                e.printStackTrace();
            }
        }
        super.onSuccessfulValidation(request, response, assertion);
    }
}
