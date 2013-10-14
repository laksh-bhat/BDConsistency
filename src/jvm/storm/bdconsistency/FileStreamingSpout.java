package bdconsistency;

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
    private String fileName;

    public FileStreamingSpout(String fileName) {
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
            Thread.sleep(10);
            if (!scanner.hasNextLine())
                scanner = new Scanner(fileName);

            _collector.emit(new Values(scanner.nextLine()));
        } catch (InterruptedException ignore) {}
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        scanner.close();
    }
}
