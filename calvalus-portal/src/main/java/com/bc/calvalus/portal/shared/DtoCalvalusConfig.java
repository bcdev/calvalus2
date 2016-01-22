package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * GWT-serializable version of a string map.
 *
 * @author boe
 */
public class DtoCalvalusConfig implements IsSerializable {
    private String user;
    private String[] roles;

    public String getUser() {
        return user;
    }

    public String[] getRoles() {
        return roles;
    }

    private Map<String, String> config;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoCalvalusConfig() {
    }

    public DtoCalvalusConfig(String user, String[] roles, Map<String, String> config) {
        if (user == null) {
                    throw new NullPointerException("user");
                }
        if (roles == null) {
                    throw new NullPointerException("roles");
                }
        if (config == null) {
                    throw new NullPointerException("config");
                }
        this.user = user;
        this.roles = roles;
        this.config = new HashMap<String, String>(config);
    }

    public Map<String, String> getConfig() {
        return Collections.unmodifiableMap(config);
    }
}
