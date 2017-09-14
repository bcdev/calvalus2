package com.bc.calvalus.portal.server;

import org.opensaml.saml2.core.Assertion;
import org.w3c.dom.Element;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

public class SAMLTokenFetchFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        if (servletRequest instanceof HttpServletRequest) {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            System.out.println(Arrays.toString(Collections.list(request.getSession().getAttributeNames()).toArray()));
            Object assertionObj = request.getSession().getAttribute("_const_cas_assertion_");
            if (assertionObj != null) {
                try {
                    Assertion assertion = (Assertion) assertionObj;
                    Element doc = assertion.getDOM();
                    Transformer transformer = null;
                    transformer = TransformerFactory.newInstance().newTransformer();
                    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
                    StreamResult result = new StreamResult(new StringWriter());
                    DOMSource source = new DOMSource(doc);
                    transformer.transform(source, result);
                    String xmlString = result.getWriter().toString();
                    System.out.println(xmlString);
                } catch (TransformerException e) {
                    e.printStackTrace();
                }
            }


            //            Cookie[] cookies = request.getCookies();
//            if (cookies != null) {
//                System.out.println("#cookies: " + cookies.length);
//                for (Cookie cookie : cookies) {
//                    System.out.println(cookie.getName() + ": " + cookie.getValue());
//                }
//            }
//            System.out.println(Arrays.toString(Collections.list(request.getSession().getAttributeNames()).toArray()));
//            request.getSession().
//        }
//        if (servletResponse instanceof HttpServletResponse) {
//            HttpServletResponse response = (HttpServletResponse) servletResponse;
//            System.out.println(Arrays.toString(response.getHeaderNames().toArray()));
//        }

//            if (request.getQueryString() != null && request.getQueryString().contains("ticket")) {
//                String ticket = request.getParameter("ticket");
//                System.out.println(ticket);
//                if (ticket != null) {
//                    String samlToken = fetchSAMLToken(ticket);
//                    System.out.println(samlToken);
//                }
//            }
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }

    private String fetchSAMLToken(String ticket) {
//        String baseUrl = "https://tsedos.eoc.dlr.de/cas/serviceValidate?ticket=";
        String baseUrl = "https://tsedos.eoc.dlr.de/cas/samlCreate2?st=";
        StringBuilder samlTokenRequestBuilder = new StringBuilder(baseUrl);
        samlTokenRequestBuilder.append(ticket);
        samlTokenRequestBuilder.append("&serviceUrl=");
//        samlTokenRequestBuilder.append("&service=");
//        samlTokenRequestBuilder.append("http%3A%2F%2Fcd-cvportal%3A8080%2Fcalvalus-portal%2F");
        samlTokenRequestBuilder.append("http://cd-cvmaster:8080/calvalus-portal/calvalus.jsp");
        StringBuilder samlTokenBuilder = new StringBuilder();
        try {
            URL url = new URL(samlTokenRequestBuilder.toString());
            try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    samlTokenBuilder.append(inputLine);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to fetch SAML token.", e);
        }

        return samlTokenBuilder.toString();
    }

    @Override
    public void destroy() {

    }
}
