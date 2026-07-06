package com.bc.calvalus.api;

import com.bc.calvalus.api.model.BackendConfig;
import org.apache.catalina.realm.UserDatabaseRealm.UserDatabasePrincipal;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.List;

public class Utils {

    public static String[] getUserRoles(HttpServletRequest request) throws NoSuchMethodException {
        final Principal userPrincipal = request.getUserPrincipal();
        if (userPrincipal instanceof UserDatabasePrincipal) {
            UserDatabasePrincipal genericPrincipal = (UserDatabasePrincipal) userPrincipal;
            final String[] roles = genericPrincipal.getRoles();
            return roles;
        } else {
            throw new NoSuchMethodException("cannot determine roles: " + userPrincipal.getClass());
        }
    }

    public static String getUserName(HttpServletRequest request, ServletContext context) throws ServletException {
        String userName = request.getUserPrincipal().getName();
        String remoteUser = request.getHeader(BackendConfig.getConfig(context).getConfigMap().get("calvalus.wps.deputy.attribute"));
        if (remoteUser != null) {
            if (userName.equals(BackendConfig.getConfig(context).getConfigMap().get("calvalus.wps.deputy.user"))) {
                return BackendConfig.getConfig(context).getConfigMap().get("calvalus.wps.deputy.prefix") + remoteUser;
            }
        }
        return userName;
    }

    /** Returns first non-form-field file item */
    public static FileItem getFileItem(HttpServletRequest request) throws FileUploadException {
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        List<FileItem> items = upload.parseRequest(request);
        for (FileItem item : items) {
            if (!item.isFormField()) {
                return item;
            }
        }
        return null;
    }
}
