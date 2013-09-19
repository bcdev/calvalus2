package com.bc.calvalus.commons;

import org.esa.beam.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marco Peters
 */
public class BundleFilter {

    private static final char VALUE_SEPARATOR = ',';
    private static final char PARAMETER_SEPARATOR = ';';
    private static final char TAG_SEPARATOR = '=';
    private static final String PROVIDER_TAG = "provider" + TAG_SEPARATOR;
    private static final String BUNDLE_TAG = "bundle" + TAG_SEPARATOR;
    private static final String PROCESSOR_TAG = "processor" + TAG_SEPARATOR;
    private static final String USER_TAG = "user" + TAG_SEPARATOR;
    private final List<Provider> providerList;
    private String bundleName;
    private String bundleVersion;
    private String processorName;
    private String processorVersion;
    private String userName;

    public enum Provider {
        SYSTEM,
        USER,
        ALL_USERS
    }


    public BundleFilter() {
        this.providerList = new ArrayList<Provider>();
    }


    public boolean isProviderSupported(Provider provider) {
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

    public BundleFilter withProvider(Provider provider) {
        providerList.add(provider);
        return this;
    }

    public BundleFilter withTheUser(String userName) {
        this.userName = userName;
        return this;
    }

    public BundleFilter withTheBundle(String bundleName, String bundleVersion) {
        if (StringUtils.isNullOrEmpty(bundleName) || StringUtils.isNullOrEmpty(bundleVersion)) {
            throw new IllegalArgumentException("bundleName and bundleVersion must not be null or empty");
        }
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        return this;
    }

    public BundleFilter withTheProcessor(String processorName, String processorVersion) {
        if (StringUtils.isNullOrEmpty(processorName) || StringUtils.isNullOrEmpty(processorVersion)) {
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
            for (Provider provider : providerList) {
                if (sb.charAt(sb.length() - 1) == TAG_SEPARATOR) {
                    sb.append(VALUE_SEPARATOR);
                }
                sb.append(provider.name());
            }
        }
        sb.append(PARAMETER_SEPARATOR);
        if (bundleName != null) {
            sb.append(BUNDLE_TAG);
            sb.append(bundleName);
            sb.append(VALUE_SEPARATOR);
            sb.append(bundleVersion);
        }
        sb.append(PARAMETER_SEPARATOR);
        if (processorName != null) {
            sb.append(PROCESSOR_TAG);
            sb.append(processorName);
            sb.append(VALUE_SEPARATOR);
            sb.append(processorVersion);
        }
        sb.append(PARAMETER_SEPARATOR);
        if (userName != null) {
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
                    filter.withProvider(Provider.valueOf(provider));
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
}
