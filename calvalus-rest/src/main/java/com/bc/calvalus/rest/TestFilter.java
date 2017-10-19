package com.bc.calvalus.rest;

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

/**
 * @author hans
 */
public class TestFilter implements Filter {

    private FilterConfig config;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        System.out.println("========= TestFilter initialized =========");
        this.config = filterConfig;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        ServletContext servletContext = this.config.getServletContext();
        ServletContext calvalusReactContext = servletContext.getContext("/calvalus-react");
        String userName = "";
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            userName = servletRequest.getUserPrincipal().getName();
            System.out.println("userName = " + userName);
        }
        if (calvalusReactContext == null) {
            System.out.println("context null");
            return;
        }
        RequestDispatcher requestDispatcher = request.getRequestDispatcher("index.html");
        if (requestDispatcher == null) {
            System.out.println("dispatcher null");
            return;
        }
        HttpServletResponse servletResponse = (HttpServletResponse) response;
        Cookie userCookie = new Cookie("userName", userName);
        servletResponse.addCookie(userCookie);
        servletResponse.sendRedirect("index.html");
//        requestDispatcher.forward(request, response);
//        RequestDispatcher requestDispatcher = request.getRequestDispatcher("/index.html");
//        requestDispatcher.forward(request, response);
    }

    @Override
    public void destroy() {
        System.out.println("========= TestFilter destroyed =========");
    }
}
