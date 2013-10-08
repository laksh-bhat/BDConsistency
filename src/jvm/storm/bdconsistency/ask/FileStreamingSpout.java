package bdconsistency.ask;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A Spout that emits fake tweets. It calculates a random probability distribution for hashtags and actor activity. It
 * uses a dataset of 500 english sentences. It has a fixed set of actors and subjects which you can also modify at your own will.
 * Tweet text is one of the random 500 sentences followed by a hashtag of one subject.
 *
 * @author pere
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class FileStreamingSpout implements IBatchSpout {
    private int batchSize;
    private Scanner scanner;
    private String fileName = "/damsl/software/storm/code/BDConsistency/resources/big_axfinder_agenda.csv";

    public FileStreamingSpout(String fileName) throws IOException {
        this(100, fileName);
    }

    public FileStreamingSpout(int batchSize, String fileName) throws IOException {
        this.batchSize = batchSize;
        this.fileName = fileName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(Map conf, TopologyContext context) {
        System.err.println("Open Spout instance");
        try {
            scanner = new Scanner(new File(fileName));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        for(int i = 0; i < batchSize; i++) collector.emit(getNextTuple());
    }

    @Override
    public void ack(long batchId) {
        // nothing to do here
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Map getComponentConfiguration() {
        // no particular configuration here
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tradeString");
    }

    private Values getNextTuple() {
        if(!scanner.hasNextLine()) {
            scanner.close();
            scanner = new Scanner(fileName);
        }
        return new Values(scanner.nextLine());
    }
}
