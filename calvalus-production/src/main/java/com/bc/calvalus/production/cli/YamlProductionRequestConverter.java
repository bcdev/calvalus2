package com.bc.calvalus.production.cli;

import com.bc.calvalus.production.ProductionRequest;
import org.yaml.snakeyaml.Yaml;

import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by marco on 05.11.14.
 */
public class YamlProductionRequestConverter {

    private final Map<String, Object> map;

    public YamlProductionRequestConverter(Reader reader) {
        Yaml yaml = new Yaml();
        map = (Map<String, Object>) yaml.load(reader);
    }

    public ProductionRequest loadProductionRequest(String userName) {
        String productionType = (String) map.get("productionType");
        Map<String, String> parameterMap = new HashMap<>();
        Set<Map.Entry<String, Object>> entries = map.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            String key = entry.getKey();
            if (!key.equals("productionType")) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    parameterMap.put(key, ((String) value).trim());
                } else if (value instanceof Number) {
                    parameterMap.put(key, ((Number) value).toString());
                } else if (value instanceof String[]) {
                    parameterMap.put(key, arrayToString((String[]) value));
                } else if (value instanceof List) {
                    List<String> sa = (List) value;
                    parameterMap.put(key, arrayToString(sa.toArray(new String[sa.size()])));
                } else if (value == null) {
                    parameterMap.put(key, "");
                } else {
                    System.out.println("unsupported key = '" + key+ "' value = '" + value+"'");
                }
            }
        }
        return new ProductionRequest(productionType, userName, parameterMap);
    }

    static String arrayToString(String[] strs) {
        if (strs.length == 0) {
            return "";
        }
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {
            sbuf.append(",");
            sbuf.append(strs[idx].trim());
        }
        return sbuf.toString();
    }
}
