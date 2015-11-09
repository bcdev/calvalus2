package com.bc.calvalus.commons.shared;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marco Peters
 */
public class BundleFilter {

    public static final String PROVIDER_SYSTEM = "SYSTEM";
    public static final String PROVIDER_USER = "USER";
    public static final String PROVIDER_ALL_USERS = "ALL_USER";
    public static final String DUMMY_PROCESSOR_NAME = "auxiliary_bundle_without_processor";

    private static final char VALUE_SEPARATOR = ',';
    private static final char PARAMETER_SEPARATOR = ';';
    private static final char TAG_SEPARATOR = '=';
    private static final String PROVIDER_TAG = "provider" + TAG_SEPARATOR;
    private static final String BUNDLE_TAG = "bundle" + TAG_SEPARATOR;
    private static final String PROCESSOR_TAG = "processor" + TAG_SEPARATOR;
    private static final String USER_TAG = "user" + TAG_SEPARATOR;

    private final List<String> providerList;
    private String bundleName;
    private String bundleVersion;
    private String processorName;
    private String processorVersion;
    private String userName;

    public BundleFilter() {
        this.providerList = new ArrayList<String>();
    }


    public boolean isProviderSupported(String provider) {
        return providerList.contains(provider);
    }

    public int getNumSupportedProvider() {
        return providerList.size();
    }

    public String getBundleName() {
        return bundleName;
    }

    public String getBundleVersion() {
        return bundleVersion;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessorVersion() {
        return processorVersion;
    }

    public String getUserName() {
        return userName;
    }

    public BundleFilter withProvider(String provider) {
        providerList.add(provider);
        return this;
    }

    public BundleFilter withTheUser(String userName) {
        this.userName = userName;
        return this;
    }

    public BundleFilter withTheBundle(String bundleName, String bundleVersion) {
        if (isNullOrEmpty(bundleName) || isNullOrEmpty(bundleVersion)) {
            throw new IllegalArgumentException("bundleName and bundleVersion must not be null or empty");
        }
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        return this;
    }

    public BundleFilter withTheProcessor(String processorName, String processorVersion) {
        if (isNullOrEmpty(processorName) || isNullOrEmpty(processorVersion)) {
            throw new IllegalArgumentException("processorName and processorVersion must not be null or empty");
        }
        this.processorName = processorName;
        this.processorVersion = processorVersion;
        return this;
    }

    /**
     * @return a string like: "provider=SYSTEM,USER;bundle=coastcolour,1.6;processor=idepix,1.8-cc;user=Hugo"
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (!providerList.isEmpty()) {
            sb.append(PROVIDER_TAG);
            for (int i = 0; i < providerList.size(); i++) {
                sb.append(providerList.get(i));
                if (i < providerList.size() - 1) {
                    sb.append(VALUE_SEPARATOR);
                }
            }
        }
        if (bundleName != null) {
            sb.append(PARAMETER_SEPARATOR);
            sb.append(BUNDLE_TAG);
            sb.append(bundleName);
            sb.append(VALUE_SEPARATOR);
            sb.append(bundleVersion);
        }
        if (processorName != null) {
            sb.append(PARAMETER_SEPARATOR);
            sb.append(PROCESSOR_TAG);
            sb.append(processorName);
            sb.append(VALUE_SEPARATOR);
            sb.append(processorVersion);
        }
        if (userName != null) {
            sb.append(PARAMETER_SEPARATOR);
            sb.append(USER_TAG);
            sb.append(userName);
        }
        return sb.toString();
    }

    public static BundleFilter fromString(String text) {
        final String[] parameters = text.split(String.valueOf(PARAMETER_SEPARATOR));

        final BundleFilter filter = new BundleFilter();
        for (String parameter : parameters) {
            if (parameter.startsWith(PROVIDER_TAG)) {
                final String[] providers = parameter.substring(PROVIDER_TAG.length()).split(String.valueOf(VALUE_SEPARATOR));
                for (String provider : providers) {
                    filter.withProvider(provider);
                }
            } else if (parameter.startsWith(BUNDLE_TAG)) {
                final String[] bundleInfos = parameter.substring(BUNDLE_TAG.length()).split(String.valueOf(VALUE_SEPARATOR));
                filter.withTheBundle(bundleInfos[0], bundleInfos[1]);
            } else if (parameter.startsWith(PROCESSOR_TAG)) {
                final String[] processorInfos = parameter.substring(PROCESSOR_TAG.length()).split(String.valueOf(VALUE_SEPARATOR));
                filter.withTheProcessor(processorInfos[0], processorInfos[1]);
            } else if (parameter.startsWith(USER_TAG)) {
                filter.withTheUser(parameter.substring(USER_TAG.length()));
            } else {
                throw new IllegalArgumentException("Tag provider=, bundle= or processor= not found in '" + text + "'");
            }
        }
        return filter;
    }

    private static boolean isNullOrEmpty(String str) {
        return str == null || str.length() == 0;
    }

}
