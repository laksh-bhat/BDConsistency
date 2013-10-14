package bdconsistency;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;


/**
 * User: lbhat@damsl
 * Date: 10/14/13
 * Time: 4:33 PM
 */
public class CounterState implements State {

    public static class CounterStateFactory implements StateFactory {

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new CounterState();
        }
    }


    private Long count;

    public CounterState() {
        count = (long) 0;
    }

    public void increment(){
        System.out.println("incrementing " + count);
        count++;
    }

    public long getCount(){
        return count;
    }

    @Override
    public void beginCommit(Long txid) {}

    @Override
    public void commit(Long txid) {}
}
