package com.bc.calvalus.wpsrest.listener;

import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProductionService;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.Timer;

/**
 * In this case, any actions (if any) to happen when the servlet context is
 * initialized or destroyed are defined.
 * <p/>
 * Created by hans on 13/10/2015.
 */
public class ContainerInitAndDestroy implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("*****************************************");
        System.out.println("****** Starting calwps application ******");
        System.out.println("*****************************************");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        Timer statusObserver = CalvalusProductionService.getStatusObserverSingleton();
        System.out.println("********************************************");
        System.out.println("******Stopping StatusObserver thread *******");
        System.out.println("********************************************");
        statusObserver.cancel();
    }
}
