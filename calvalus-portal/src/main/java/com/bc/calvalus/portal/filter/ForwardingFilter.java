package com.bc.calvalus.portal.filter;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class ForwardingFilter implements Filter {

    private FilterConfig config;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        this.config = filterConfig;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        ServletContext servletContext = this.config.getServletContext();
        ServletContext calvalusPortalContext = servletContext.getContext("/calvalus-portal");
        List<Cookie> inputSelectionCookies = new ArrayList<>();
        Cookie[] oldCookies = new Cookie[0];
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            oldCookies = servletRequest.getCookies();
            for (Cookie oldCookie : oldCookies) {
                if(!"JSESSIONID".equalsIgnoreCase(oldCookie.getName())){
                    oldCookie.setValue(null);
                    oldCookie.setMaxAge(0);
                }
            }
            StringWriter writer = new StringWriter();
            IOUtils.copy(servletRequest.getInputStream(), writer);
            String inputSelectionString = writer.toString();
            Gson gson = new Gson();
            InputSelection inputSelection = gson.fromJson(inputSelectionString, InputSelection.class);
            if(inputSelection.getCollectionName() != null) inputSelectionCookies.add(new Cookie("collectionName", inputSelection.getCollectionName()));
            if(inputSelection.getProductIdentifiers() != null) inputSelectionCookies.add(new Cookie("productIdentifiers", inputSelection.getProductIdentifiers().toString()));
            if (inputSelection.getDateRange() != null) {
                inputSelectionCookies.add(new Cookie("startTime", inputSelection.getDateRange().getStartTime()));
                inputSelectionCookies.add(new Cookie("endTime", inputSelection.getDateRange().getEndTime()));
            }
            if(inputSelection.getRegionGeometry() != null) inputSelectionCookies.add(new Cookie("regionGeometry", inputSelection.getRegionGeometry()));
        }
        if (calvalusPortalContext == null) {
            System.out.println("context null");
            return;
        }
        RequestDispatcher requestDispatcher = request.getRequestDispatcher("calvalus.jsp");
        if (requestDispatcher == null) {
            System.out.println("dispatcher null");
            return;
        }
        HttpServletResponse servletResponse = (HttpServletResponse) response;
        for (Cookie oldCookie : oldCookies) {
            servletResponse.addCookie(oldCookie);
        }
        for (Cookie inputSelectionCookie : inputSelectionCookies) {
            servletResponse.addCookie(inputSelectionCookie);
        }
        requestDispatcher.forward(request, servletResponse);
    }

    @Override
    public void destroy() {
        System.out.println("========= ForwardingFilter destroyed =========");
    }
}
