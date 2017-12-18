package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringWriter;

/**
 * @author hans
 */
public class InjectInputSelectionServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
                throws IOException {
        StringWriter writer = new StringWriter();
        try (ServletInputStream inputStream = servletRequest.getInputStream()) {
            IOUtils.copy(inputStream, writer);
        }
        String inputSelectionString = writer.toString();
        Gson gson = new Gson();
        DtoInputSelection dtoInputSelection = gson.fromJson(inputSelectionString, DtoInputSelection.class);
        getServletContext().setAttribute("catalogueSearch", dtoInputSelection);
    }
}
