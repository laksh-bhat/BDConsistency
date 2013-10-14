package bdconsistency.query;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 7:03 PM
 */
public class AxFinderFilter {

    public static class AsksFilter  extends BaseFilter{
        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getStringByField("tradeString").startsWith("ASKS");
        }
    }

    public static class BidsFilter  extends BaseFilter{
        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getStringByField("tradeString").startsWith("BIDS");
        }
    }

    public static class DummyFilter extends BaseFilter{
        @Override
        public boolean isKeep(TridentTuple tuple) {
            if(tuple.getString(0).length() > 0)
                return true;
            else
                return false;
        }
    }
}
