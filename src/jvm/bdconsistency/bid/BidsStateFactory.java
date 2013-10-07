package jvm.bdconsistency.bid;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 12:02 AM
 */
public class BidsStateFactory implements StateFactory {
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new BidsState();
    }
}
