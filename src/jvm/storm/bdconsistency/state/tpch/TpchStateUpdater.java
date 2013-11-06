package bdconsistency.state.tpch;

import bdconsistency.tpchschema.TpchAgenda;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class TpchStateUpdater extends BaseStateUpdater<TpchState> {


    @Override
    public void updateState (final TpchState state, final List<TridentTuple> tuples, final TridentCollector collector) {
        for (TridentTuple tuple : tuples){
            String tableName = tuple.getStringByField("table").intern();
            state.updateTable(tableName, (TpchAgenda) tuple.getValueByField("agendaObject"));
        }
    }
}
