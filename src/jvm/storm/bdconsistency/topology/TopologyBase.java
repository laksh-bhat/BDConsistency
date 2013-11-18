package bdconsistency.topology;

import backtype.storm.utils.DRPCClient;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * User: lbhat@damsl
 * Date: 10/30/13
 * Time: 7:51 PM
 */
public class TopologyBase {
    public static int NUM_QUERIES = 10;
    public static void checkArguments(String[] args) {
        if (args.length < 3){
            System.out.println("args -- filename , statesize , sleeptime (30000 - 60000 ms)");
            System.exit(0);
        }
        if (args.length > 3)
            NUM_QUERIES = Integer.valueOf(args[3]);
    }

    public static void printTimings(long duration, int numQueries) {
        System.out.println("==================================================================");
        System.out.println(MessageFormat.format("duration for {1} queries {0} mill seconds",
                duration, numQueries));
    }

    public static void cleanup(DRPCClient client, BufferedWriter writer) throws IOException {
        writer.close();
        client.close();
    }

    public static void cleanup(DRPCClient client) {
        client.close();
    }

}
