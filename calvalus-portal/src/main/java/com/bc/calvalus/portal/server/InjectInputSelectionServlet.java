package com.bc.calvalus.portal.server;

import static com.bc.calvalus.portal.server.BCAuthenticationFilter.PAYLOAD_PREFIX;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gson.Gson;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.security.Principal;

/**
 * @author hans
 */
public class InjectInputSelectionServlet extends HttpServlet {

    private static final String FORWARD_URL = "/close-window.jsp";
    private static final String CATALOGUE_SEARCH_PREFIX = "catalogueSearch_";

    @Override
    protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        String requestPayload = servletRequest.getParameter("request");
        System.out.println("Request payload: " + requestPayload);
        DtoInputSelection dtoInputSelection = getDtoInputSelectionFromJson(requestPayload);
        Principal userPrincipal = servletRequest.getUserPrincipal();
        if (userPrincipal != null) {
            getServletContext().setAttribute(CATALOGUE_SEARCH_PREFIX + userPrincipal.getName(), dtoInputSelection);
        }
        closeBrowserWindow(servletRequest, servletResponse);
    }

    @Override
    protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        HttpSession session = servletRequest.getSession();
        String payload = "";
        if (session != null) {
            String sessionId = session.getId();
            payload = (String) session.getAttribute(PAYLOAD_PREFIX + sessionId);
            System.out.println("Request payload: " + payload);
        }
        DtoInputSelection dtoInputSelection = getDtoInputSelectionFromJson(payload);
        Principal userPrincipal = servletRequest.getUserPrincipal();
        if (userPrincipal != null) {
            getServletContext().setAttribute(CATALOGUE_SEARCH_PREFIX + userPrincipal.getName(), dtoInputSelection);
        }
        closeBrowserWindow(servletRequest, servletResponse);
    }

    private void closeBrowserWindow(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        RequestDispatcher rd = getServletContext().getRequestDispatcher(FORWARD_URL);
        try {
            rd.forward(servletRequest, servletResponse);
        } catch (ServletException | IOException exception) {
            System.err.println("Unable to forward to " + FORWARD_URL);
            exception.printStackTrace();
        }
    }

    private DtoInputSelection getDtoInputSelectionFromJson(String payload) {
        Gson gson = new Gson();
        return gson.fromJson(payload, DtoInputSelection.class);
    }
}
