package com.bc.calvalus.production;

import com.bc.calvalus.commons.BundleFilter;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.BundleDescriptor;

import java.io.OutputStream;

/**
 * The interface to the Calvalus production service.
 *
 * @author Norman
 */
public interface ProductionService {

    /**
     * Gets all known product sets which match the given filter.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The product sets.
     *
     * @throws ProductionException If a service error occurred.
     */
    ProductSet[] getProductSets(String filter) throws ProductionException;

    /**
     * Gets all known bundles which match the given filter.
     *
     * @param filter A filter to define which bundle is searched for.
     *
     * @return The processors.
     *
     * @throws ProductionException If a service error occurred.
     */
    BundleDescriptor[] getBundles(BundleFilter filter) throws ProductionException;

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
    void updateStatuses();

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     */
    void close() throws ProductionException;

    ////////////////////////////////////////////////////////////////////////////////////////
    // Facade for special inventory usages.

    /**
     * Lists files within the user's file space in the inventory.
     *
     * @param userName The name of an authorised user.
     * @param glob     A glob that may contain
     *
     * @return The listing of files.
     *
     * @throws ProductionException If an error occured.
     */
    String[] listUserFiles(String userName, String glob) throws ProductionException;

    /**
     * Creates a file from the user's file space in the inventory.
     *
     * @param userName The name of an authorised user.
     * @param path     A relative path into the user's file space.
     *
     * @return An output stream.
     *
     * @throws ProductionException If an error occured.
     */
    OutputStream addUserFile(String userName, String path) throws ProductionException;

    /**
     * Deletes a file from the user's file space in the inventory.
     *
     * @param userName The name of an authorised user.
     * @param path     A relative path into the user's file space.
     *
     * @return true, if the file could be found and removed.
     *
     * @throws ProductionException If an error occured (file exists, but can't be removed).
     */
    boolean removeUserFile(String userName, String path) throws ProductionException;

    String getQualifiedUserPath(String userName, String filePath);
}
