package com.bc.calvalus.portal.server;

import static com.bc.calvalus.portal.server.BCAuthenticationFilter.PAYLOAD_PREFIX;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gson.Gson;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * @author hans
 */
public class InjectInputSelectionServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        String requestPayload = servletRequest.getParameter("request");
        DtoInputSelection dtoInputSelection = getDtoInputSelectionFromJson(requestPayload);
        getServletContext().setAttribute("catalogueSearch", dtoInputSelection);
    }

    @Override
    protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        HttpSession session = servletRequest.getSession();
        String payload = "";
        if (session != null) {
            String sessionId = session.getId();
            payload = (String) session.getAttribute(PAYLOAD_PREFIX + sessionId);
        }
        DtoInputSelection dtoInputSelection = getDtoInputSelectionFromJson(payload);
        getServletContext().setAttribute("catalogueSearch", dtoInputSelection);
    }

    private DtoInputSelection getDtoInputSelectionFromJson(String payload) {
        Gson gson = new Gson();
        return gson.fromJson(payload, DtoInputSelection.class);
    }
}
