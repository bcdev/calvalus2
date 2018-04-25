package com.bc.calvalus.wps.authentication;

import org.apache.commons.io.IOUtils;
import org.jasig.cas.client.authentication.DefaultGatewayResolverImpl;
import org.jasig.cas.client.authentication.GatewayResolver;
import org.jasig.cas.client.util.AbstractCasFilter;
import org.jasig.cas.client.util.CommonUtils;
import org.jasig.cas.client.validation.Assertion;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author hans
 */
public class WpsAuthenticationFilter extends AbstractCasFilter {

    private static final String PAYLOAD_PREFIX = "wps_payload_";
    static final String WPS_REQUEST_URL_PREFIX = "wps_request_type_";
    /**
     * The URL to the CAS Server login.
     */
    private String casServerLoginUrl;

    /**
     * Whether to send the renew request or not.
     */
    private boolean renew = false;

    /**
     * Whether to send the gateway request or not.
     */
    private boolean gateway = false;

    private GatewayResolver gatewayStorage = new DefaultGatewayResolverImpl();

    protected void initInternal(final FilterConfig filterConfig) throws ServletException {
        if (!isIgnoreInitConfiguration()) {
            super.initInternal(filterConfig);
            setCasServerLoginUrl(getPropertyFromInitParams(filterConfig, "casServerLoginUrl", null));
            log.trace("Loaded CasServerLoginUrl parameter: " + this.casServerLoginUrl);
            setRenew(parseBoolean(getPropertyFromInitParams(filterConfig, "renew", "false")));
            log.trace("Loaded renew parameter: " + this.renew);
            setGateway(parseBoolean(getPropertyFromInitParams(filterConfig, "gateway", "false")));
            log.trace("Loaded gateway parameter: " + this.gateway);

            final String gatewayStorageClass = getPropertyFromInitParams(filterConfig, "gatewayStorageClass", null);

            if (gatewayStorageClass != null) {
                try {
                    this.gatewayStorage = (GatewayResolver) Class.forName(gatewayStorageClass).newInstance();
                } catch (final Exception e) {
                    log.error(e, e);
                    throw new ServletException(e);
                }
            }
        }
    }

    @Override
    public void init() {
        super.init();
        CommonUtils.assertNotNull(this.casServerLoginUrl, "casServerLoginUrl cannot be null.");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
                throws IOException, ServletException {
        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;
        final HttpSession session = request.getSession(true);
        final Assertion assertion = session != null ? (Assertion) session.getAttribute(CONST_CAS_ASSERTION) : null;

        if (assertion != null) {
            filterChain.doFilter(request, response);
            return;
        }

        ServletInputStream inputStream = request.getInputStream();
        String requestPayload = IOUtils.toString(inputStream);
        IOUtils.closeQuietly(inputStream);
        String requestURI = request.getRequestURI();
        String queryString = request.getQueryString();
        System.out.println("requestPayload : " + requestPayload);
        System.out.println("request.getQueryString() : " + queryString);
        System.out.println("requestURI : " + requestURI);
        System.out.println("session : " + session);
        String requestMethod = request.getMethod();
        System.out.println("request.getMethod() : " + requestMethod);

        if (session != null) {
            String sessionId = session.getId();
            if ("GET".equalsIgnoreCase(requestMethod)
                && queryString != null
                && queryString.contains("Service=WPS")) {
                String wpsRequestURL = WPS_REQUEST_URL_PREFIX;
                System.out.println("Setting " + wpsRequestURL + " : " + "?" + queryString);
                session.setAttribute(wpsRequestURL, "?" + queryString);
                response.addCookie(new Cookie("queryString", queryString));
            } else if ("POST".equalsIgnoreCase(requestMethod)
                       && requestPayload != null) {
                String payloadKey = PAYLOAD_PREFIX;
                System.out.println("Setting " + payloadKey + " : " + requestPayload);
                Cookie[] cookies = request.getCookies();
                for (Cookie cookie : cookies) {
                    if ("requestId".equalsIgnoreCase(cookie.getName())) {
                        String id = cookies[0].getValue();
                        try (FileWriter fw = new FileWriter(
                                    new File("/tmp/" + "request-" + id));) {
                            fw.write(requestPayload);
                        }
                    }
                }
//                session.setAttribute(payloadKey, requestPayload);
            }
        }

        final String serviceUrl = constructServiceUrl(request, response);
        final String ticket = CommonUtils.safeGetParameter(request, getArtifactParameterName());
        final boolean wasGatewayed = this.gatewayStorage.hasGatewayedAlready(request, serviceUrl);

        if (CommonUtils.isNotBlank(ticket) || wasGatewayed) {
            filterChain.doFilter(request, response);
            return;
        }

        final String modifiedServiceUrl;

        log.debug("no ticket and no assertion found");
        if (this.gateway) {
            log.debug("setting gateway attribute in session");
            modifiedServiceUrl = this.gatewayStorage.storeGatewayInformation(request, serviceUrl);
        } else {
            modifiedServiceUrl = serviceUrl;
        }

        if (log.isDebugEnabled()) {
            log.debug("Constructed service url: " + modifiedServiceUrl);
        }

        final String urlToRedirectTo = CommonUtils.constructRedirectUrl(this.casServerLoginUrl,
                                                                        getServiceParameterName(), modifiedServiceUrl,
                                                                        this.renew, this.gateway);

        if (log.isDebugEnabled()) {
            log.debug("redirecting to \"" + urlToRedirectTo + "\"");
        }

        response.sendRedirect(urlToRedirectTo);

    }

    public final void setRenew(final boolean renew) {
        this.renew = renew;
    }

    public final void setGateway(final boolean gateway) {
        this.gateway = gateway;
    }

    public final void setCasServerLoginUrl(final String casServerLoginUrl) {
        this.casServerLoginUrl = casServerLoginUrl;
    }
}
