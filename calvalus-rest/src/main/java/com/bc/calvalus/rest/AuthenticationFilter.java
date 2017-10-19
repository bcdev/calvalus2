package com.bc.calvalus.rest;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;

/**
 * @author hans
 */
public class AuthenticationFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        System.out.println("========= Auth Filter =========");
        String userName = requestContext.getSecurityContext().getUserPrincipal().getName();
        System.out.println("userName = " + userName);
        System.out.println("========= Auth Filter =========");
    }
}
