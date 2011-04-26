package com.bc.calvalus.binning;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * A simple registry for {@link AggregatorDescriptor}s.
 *
 * @author MarcoZ
 * @author Norman
 */
public class AggregatorDescriptorRegistry {
    private final HashMap<String, AggregatorDescriptor> map;

    private AggregatorDescriptorRegistry() {
        map = new HashMap<String, AggregatorDescriptor>();
        for (AggregatorDescriptor descriptor : ServiceLoader.load(AggregatorDescriptor.class)) {
            map.put(descriptor.getName(), descriptor);
        }
    }

    public static AggregatorDescriptorRegistry getInstance() {
        return Holder.instance;
    }

    public AggregatorDescriptor getAggregatorDescriptor(String name) {
        return map.get(name);
    }

    // Initialization-on-demand holder idiom
    private static class Holder {
        private static final AggregatorDescriptorRegistry instance = new AggregatorDescriptorRegistry();
    }
}
