package utils_lod;

import java.util.Set;

public class TextUtils {

    /**
     * @param values
     * @param value
     * @return
     */
    public static boolean isTermContainedInSet(Set<String> values, String value) {
        if (values == null || values.isEmpty())
            return true;

        for (String valuecmp : values) {
            if (value.toLowerCase().trim().contains(valuecmp.toLowerCase().trim())) {
                return true;
            }
        }
        return false;
    }

}
