package edu.fudan.storm.stormtest.topology;

import edu.fudan.storm.LoadMonitor.TaskMonitor;
import edu.fudan.storm.LoadMonitor.WorkerMonitor;
import edu.fudan.storm.metrichook.SchedulingMetricsToZookeeperWriter;
import edu.fudan.storm.stormtest.utils.TestUtil;
import edu.fudan.storm.tools.metrics.BasicMetricsCollector;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ParallelTopology {

    @Option(name="--topologyname", aliases={"-name"}, metaVar="topology name",
            usage="name of the topology")
    private String topoName = "ParallelTopology";

    @Option(name="--parallelism", aliases={"-p"}, metaVar="PARALLELISM",
            usage="number of spouts/bolts to generate on each level")
    private int parallelism = 1;

    @Option(name="--depth", aliases={"-d"}, metaVar="DEPTH",
            usage="number of bolts to concatenate to each other.")
    private int depth = 1;

    @Option(name="--workers", aliases={"-w"}, metaVar="WORKERS",
            usage="number of workers to assign to the topology")
    private int numWorkers = 1;

    @Option(name="--numAckers", aliases={"-a"}, metaVar="A",
            usage="the number of acker tasks to start.")
    private int numAckers = 0;

    @Option(name="--runtime",aliases = {"-rt"})
    private  int runtime=5;

    @Option(name = "--rate",aliases = {"-rate"},usage = "Spout sending rate in k/s")
    private int rate=10;

    @Option(name = "--enableHScheduler", aliases = {"-eh"})
    private boolean enableHScheduler=false;

    @Option(name = "--factor", aliases = {"-fr"})
    private int factor=1;

    @Option(name = "--max_pending", aliases = {"-mp"})
    private int maxPending=100000;

    public static void main(String[] args) {
        new ParallelTopology().realMain(args);
    }
    private void realMain(String[] args) {

        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
        }

        //put cluster conf into topology conf
        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf)
                .getClient();
        Config conf = new Config();
        conf.putAll(clusterConf);
        //put topology name into conf
        String topo_name = this.topoName;
        conf.put(Config.TOPOLOGY_NAME, topo_name);
        //set metric store file path
        String resultPath = TestUtil.result_path + "/ParallelTopology/";
        new File(resultPath).mkdirs();
        conf.put("metrics.path", resultPath);
        conf.put("metrics.time", this.runtime);
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("uuid-spout", new UuidSpout(this.enableHScheduler, this.rate / this.parallelism, this.factor), this.parallelism);
        String lastComponent = "uuid-spout";
        for(int i=1;i<=this.depth;i++) {
            String currentComponent = "nothing-bolt-"+i;
            b.setBolt(currentComponent, new NothingBolt(this.enableHScheduler), this.parallelism).fieldsGrouping(lastComponent, new Fields("keyfield"));
            lastComponent = currentComponent;
        }
        if (this.enableHScheduler) {
            List<String> taskHooks = new ArrayList<>();
            taskHooks.add("edu.fudan.storm.metrichook.SchedulingMetricsCollectionHook");
            conf.put("topology.auto.task.hooks", taskHooks);
            conf.registerMetricsConsumer(SchedulingMetricsToZookeeperWriter.class);
        }
        conf.setNumWorkers(this.numWorkers);
        conf.setNumAckers(this.numAckers);
        conf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, this.maxPending);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,10);

        StormTopology topo =b.createTopology();
        try {
            StormSubmitter.submitTopology(topo_name, conf, topo);
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

        //build up metric collector
        String suffix = "";
        if(this.enableHScheduler)
            suffix="_eh";
        BasicMetricsCollector collector=new BasicMetricsCollector(conf,topo,"_p"+this.parallelism+"_d"+this.depth+"_f"+this.factor+"_w"+this.numWorkers+suffix);
        collector.run();
        try {
            System.out.println("killing topology" +topo_name);
            KillOptions ko=new KillOptions();
            ko.set_wait_secs(0);
            client.killTopologyWithOpts( topo_name , ko);
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    private static class UuidSpout extends BaseRichSpout {

        private final Logger LOG = LoggerFactory.getLogger(getClass());

        private String uuid;

        private SpoutOutputCollector collector;

        private long emitCount;

        int parallelism;

        private int thisTaskIndex;

        private TaskMonitor taskMonitor;

        private WorkerMonitor workerMonitor;

        private int spoutRate;

        private long lastSleepTime;

        /**
         * If this is set to true, the statistics for Aniello's scheduler will not be collected.
         */
        boolean enableAScheduler;

        String payload;

        int factor;

        UuidSpout(boolean enableAScheduler, int rate, int factor) {
            this.enableAScheduler = enableAScheduler;
            this.spoutRate=rate*1000;
            this.factor = factor;
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("keyfield", "payload"));
        }


        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            MessageDigest md;
            int counter;

            this.thisTaskIndex = context.getThisTaskIndex();
            this.parallelism = context.getComponentTasks(context.getThisComponentId()).size();
            counter = 0;

            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Couldn't find MD5 algorithm.", e);
            }

            // we want to create a message that hashes to exacly one of the following spouts. As there are the same number
            // of bolts on each level as there are spouts, we just keep looking until we find a uuid whose hash code would
            // be assigned to the id of this spout (if it were a bolt).
            do {
                if (++counter > 1000 * 1000) {
                    throw new RuntimeException("Unable to generate required UUID in 1 mio tries.");
                }
                byte[] bytes = md.digest(UUID.randomUUID().toString().getBytes());
                this.uuid = new String(bytes);
            } while (this.uuid.hashCode() % this.parallelism != this.thisTaskIndex);

            this.collector = collector;

            if (this.enableAScheduler) {
                // this will create/configure the worker monitor once per worker
                this.workerMonitor = WorkerMonitor.getInstance();
                this.workerMonitor.setContextInfo(context);
                // this object is used in the emit/execute method to compute the number of inter-node messages
                this.taskMonitor = new TaskMonitor(context.getThisTaskId());
                taskMonitor.checkThreadId();
            }
            this.payload = Strings.repeat(this.uuid, this.factor);
        }


        public void nextTuple() {
            this.emitCount++; // we start with msgId = 1
            this.collector.emit(new Values(this.uuid, this.payload), this.emitCount);
        }
    }

    private static class NothingBolt extends BaseRichBolt {

        private OutputCollector collector;

        /**
         * If this is set to true, the statistics for Aniello's scheduler will not be collected.
         */
        boolean enableAScheduler;

        TaskMonitor taskMonitor;

        WorkerMonitor workerMonitor;

        String uuid;

        int parallelism;

        int thisTaskIndex;

        NothingBolt(boolean enableAScheduler) {
            this.enableAScheduler = enableAScheduler;
        }


        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

            MessageDigest md;
            int counter;

            this.thisTaskIndex = context.getThisTaskIndex();
            this.parallelism = context.getComponentTasks(context.getThisComponentId()).size();
            counter = 0;

            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Couldn't find MD5 algorithm.", e);
            }

            // we want to create a message that hashes to exacly one of the following spouts. As there are the same number
            // of bolts on each level as there are spouts, we just keep looking until we find a uuid whose hash code would
            // be assigned to the id of this spout (if it were a bolt).
            do {
                if (++counter > 1000 * 1000) {
                    throw new RuntimeException("Unable to generate required UUID in 1 mio tries.");
                }
                byte[] bytes = md.digest(UUID.randomUUID().toString().getBytes());
                this.uuid = new String(bytes);
            } while (this.uuid.hashCode() % this.parallelism != this.thisTaskIndex);

            this.collector = collector;
            if (this.enableAScheduler) {
                // this will create/configure the worker monitor once per worker
                this.workerMonitor = WorkerMonitor.getInstance();
                this.workerMonitor.setContextInfo(context);
                // this object is used in the emit/execute method to compute the number of inter-node messages
                this.taskMonitor = new TaskMonitor(context.getThisTaskId());
                this.taskMonitor.checkThreadId();
            }
        }

        public void execute(Tuple input) {
            this.collector.emit(input, new Values(this.uuid, input.getValue(1)));
            this.collector.ack(input);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("keyfield", "payload"));
        }
    }
}
