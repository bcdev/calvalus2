package com.bc.calvalus.production;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;

import java.io.IOException;
import java.util.Observer;
import java.util.Timer;

/**
 * The interface to the Calvalus production service.
 *
 * @author Norman
 */
public interface ProductionService {

    /**
     * Gets all known bundles which match the given filter.
     *
     *
     * @param username
     * @param filter A filter to define which bundle is searched for.
     *
     * @return The processors.
     *
     * @throws ProductionException If a service error occurred.
     */
    BundleDescriptor[] getBundles(String username, BundleFilter filter) throws ProductionException;

    /**
     * Gets all masks for the given user.
     *
     * @param userName The user to fetch the masks for.
     * @return The masks of that user.
     * @throws ProductionException If a service error occurred.
     */
    MaskDescriptor[] getMasks(String userName) throws ProductionException;

    /**
     * Gets all known productions which match the given filter.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The productions.
     *
     * @throws ProductionException If a service error occurred.
     */
    Production[] getProductions(String filter) throws ProductionException;

    /**
     * Gets the production for the given ID.
     *
     * @param id The production ID.
     *
     * @return The production, or {@code null} if none with the given ID was found.
     *
     * @throws ProductionException If a service error occurred.
     */
    Production getProduction(String id) throws ProductionException;

    /**
     * Orders a new productions.
     *
     * @param request The request.
     *
     * @return The response.
     *
     * @throws ProductionException If a service error occurred.
     */
    ProductionResponse orderProduction(ProductionRequest request) throws ProductionException;

    /**
     * Orders a new productions.
     *
     * @param request The request.
     * @param jobHook a hook run before each job is submitted.
     *
     * @return The response.
     *
     * @throws ProductionException If a service error occurred.
     */
    ProductionResponse orderProduction(ProductionRequest request, HadoopJobHook jobHook) throws ProductionException;

    /**
     * Requests cancellation of productions with given IDs.
     *
     * @param productionIds The production IDs.
     *
     * @throws ProductionException If a service error occurred.
     */
    void cancelProductions(String... productionIds) throws ProductionException;

    /**
     * Requests deletion of productions with given IDs.
     *
     * @param productionIds The production IDs.
     *
     * @throws ProductionException If a service error occurred.
     */
    void deleteProductions(String... productionIds) throws ProductionException;

    /**
     * Requests stating of productions with given IDs.
     *
     * @param productionIds The production IDs.
     *
     * @throws ProductionException If a service error occurred.
     */
    void stageProductions(String... productionIds) throws ProductionException;

    /**
     * Requests to copy the production with given ID via scp to a remote host.
     *
     * @param productionId The production ID.
     * @param remotePath   The path specify the remote destination.<br/>
     *                     It must be given in the following format:
     *                     {@code <username>@<hostname>:<destinationPath>}.<br/>
     *                     On the remote host the public key of the current user be registered and the
     *                     private key must be located in the file {@code <user_home>/.ssh/id_rsa}
     *
     * @throws ProductionException If a service error occurred.
     */
    void scpProduction(String productionId, String remotePath) throws ProductionException;


    // todo - actually the service shall update itself on a regular basis (nf)

    /**
     * A request to retrieve and update the status of all workflows.
     */
    void updateStatuses(String username);

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     */
    void close() throws ProductionException;

    /**
     * Called once by service to register an observer for production events (final status change).
     * The service has to implement the Observer interface.
     * The method Observer.update(Observable productionService, Object production) will be called after staging is done.
     * It is called in a thread that can be used to write a report to a file.
     * @param observer
     */
    void addObserver(Observer observer);

    /**
     * Called once by service to de-register an observer.
     * @param observer
     */
    void deleteObserver(Observer observer);

    /**
     * Called by staging every time a job changes to a final state.
     * @param arg
     */
    void notifyObservers(Object arg);

    void setChanged();

    /**
     * Load details about the region data.
     */
    public String[][] loadRegionDataInfo(String username, String url) throws IOException;

    /**
     * Invalidates bundle cache after upload or delete
     */
    void invalidateBundleCache();

    public ProcessingService getProcessingService();
}
