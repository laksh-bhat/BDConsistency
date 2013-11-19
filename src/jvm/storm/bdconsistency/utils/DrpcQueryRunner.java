package bdconsistency.utils;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

import java.io.IOException;

import static bdconsistency.topology.TopologyBase.printTimings;

/**
 * User: lbhat@damsl
 * Date: 11/18/13
 * Time: 9:16 PM
 */
public class DrpcQueryRunner {
    public static void main (String[] args) throws IOException, TException, DRPCExecutionException {
        if (args.length < 1)
            System.err.println("Where are the arguments?");

        long duration = 0;
        DRPCClient drpcClient = new DRPCClient("localhost", 3772, 900000);
        long startTime = System.currentTimeMillis();
        String result = runQuery(args[0], drpcClient);
        long endTime = System.currentTimeMillis();
        duration += endTime - startTime;
        System.out.println(result);
        drpcClient.close();
        printTimings(duration, 1);
    }

    private static String runQuery (final String topologyAndDrpcServiceName, final DRPCClient client) throws TException, DRPCExecutionException {/*Query Arguments in order -- marketsegment, orderdate, shipdate*/
        return client.execute(topologyAndDrpcServiceName, "1080548553,19950315,19950315");
    }
}
