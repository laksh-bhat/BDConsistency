package bdconsistency.state.tpch;

import bdconsistency.tpchschema.TpchAgenda;
import storm.trident.state.State;

import java.io.Serializable;
import java.util.Set;

public interface ITpchTable extends State, Serializable {
    public String getTableName ();
    public Set<Object> getRows ();
    public void append (ITpchTable table);
    public void add (TpchAgenda agenda);
}
