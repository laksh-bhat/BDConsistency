package bdconsistency.ask;

import backtype.storm.task.IMetricsContext;
import bdconsistency.bid.BidsState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:52 PM
 */
 public class AsksStateFactory implements StateFactory {
    long statesize;
    public  AsksStateFactory(long stateSize){
        this.statesize = stateSize;
    }
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new AsksState(statesize);
    }
}
