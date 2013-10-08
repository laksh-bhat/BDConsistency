package bdconsistency.ask;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings({ "serial", "rawtypes" })
public class FileStreamingSpout implements IRichSpout {
    SpoutOutputCollector _collector;
    private Scanner scanner;
    private String fileName;// = "/damsl/software/storm/code/BDConsistency/resources/big_axfinder_agenda.csv";

    public FileStreamingSpout(String fileName) throws IOException {
        this.fileName = fileName;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tradeString"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new Config();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        System.err.println("Open Spout instance");
        try {
            scanner = new Scanner(new File(fileName));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public void activate() {
        if(scanner != null)
            scanner.close();
        scanner = new Scanner(fileName);
    }

    @Override
    public void deactivate() {
        scanner.reset();
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(100);
            _collector.emit(new Values(scanner.nextLine()));
        } catch (InterruptedException ignore) {}
    }

    @Override
    public void ack(Object msgId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void fail(Object msgId) {
        scanner.close();
    }

 /*   @SuppressWarnings("unchecked")
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
    }*/
}
