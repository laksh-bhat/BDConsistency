package bdconsistency;

import java.util.HashMap;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/5/13
 * Time: 8:22 PM
 */
public class test {
    public static void main(String[] args) {
        Map<Integer, Integer> simpleMap = new HashMap<Integer, Integer>();
        simpleMap.put(1,1);
        castMaps(simpleMap);
    }

    private static void castMaps(Object simpleMap) {
        Map<Integer, Integer> test = null;
        if(simpleMap instanceof Map)
            test = (Map<Integer, Integer>) simpleMap;

        System.out.println(test);
    }
}
