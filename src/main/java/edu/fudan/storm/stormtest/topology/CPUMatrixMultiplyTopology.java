package edu.fudan.storm.stormtest.topology;

import com.esotericsoftware.kryo.Kryo;
import edu.fudan.storm.LoadMonitor.TaskMonitor;
import edu.fudan.storm.LoadMonitor.WorkerMonitor;
import edu.fudan.storm.metrichook.SchedulingMetricsToZookeeperWriter;
import edu.fudan.storm.stormtest.utils.TestUtil;
import edu.fudan.storm.tools.metrics.BasicMetricsCollector;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
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

import java.io.File;
import java.util.*;

public class CPUMatrixMultiplyTopology {

    private static Logger LOG = Logger.getLogger(CPUMatrixMultiplyTopology.class);
    private static Random rand = new Random();

    @Option(name="--topologyname", aliases={"-name"}, metaVar="topology name",
            usage="name of the topology")
    private String topoName = "CPUMatrixMultiplyTopology";

    @Option(name="--parallelism", aliases={"-p"}, metaVar="PARALLELISM",
            usage="number of spouts/bolts to generate on each level")
    private int parallelism = 1;

    @Option(name="--workers", aliases={"-w"}, metaVar="WORKERS",
            usage="number of workers to assign to the topology")
    private int numWorkers = 1;

    @Option(name="--numAckers", aliases={"-a"}, metaVar="A",
            usage="the number of acker tasks to start.")
    private int numAckers = 0;

    @Option(name="--runtime",aliases = {"-rt"})
    private  int runtime=5;

    @Option(name = "--enableHScheduler", aliases = {"-eh"})
    private boolean enableHScheduler=false;

    @Option(name = "--max_pending", aliases = {"-mp"})
    private int maxPending=100;

    @Option(name = "--matrix_size", aliases = {"-s"})
    private int matrix_size=100;

    public static void main(String[] args) {
        new CPUMatrixMultiplyTopology().realMain(args);
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
        String resultPath = TestUtil.result_path + "/CPUMatrixMultiplyTopology/";
        new File(resultPath).mkdirs();
        conf.put("metrics.path", resultPath);
        conf.put("metrics.time", this.runtime);
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("matrix-spout", new matrixSpout(this.enableHScheduler, this.matrix_size), this.parallelism);
        b.setBolt("multiply-bolt", new mulBolt(this.enableHScheduler),this.parallelism).shuffleGrouping("matrix-spout");
        b.setBolt("aggregation-bolt", new aggregationBolt(this.enableHScheduler),this.parallelism).shuffleGrouping("multiply-bolt");
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
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            e.printStackTrace();
        }

        //build up metric collector
        String suffix = "";
        if(this.enableHScheduler)
            suffix="_eh";
        BasicMetricsCollector collector=new BasicMetricsCollector(conf,topo,"_p"+this.parallelism+"_w"+this.numWorkers+"_s"+this.matrix_size+suffix);
        collector.run();
        try {
            System.out.println("killing topology" +topo_name);
            KillOptions ko=new KillOptions();
            ko.set_wait_secs(0);
            client.killTopologyWithOpts( topo_name , ko);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public static class mulBolt extends BaseRichBolt {

        private OutputCollector collector;
        private boolean enableAScheduler;
        private WorkerMonitor workerMonitor;
        private TaskMonitor taskMonitor;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("result"));
        }

        mulBolt(boolean enableAScheduler) {
            this.enableAScheduler = enableAScheduler;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            if (this.enableAScheduler) {
                // this will create/configure the worker monitor once per worker
                this.workerMonitor = WorkerMonitor.getInstance();
                this.workerMonitor.setContextInfo(topologyContext);
                // this object is used in the emit/execute method to compute the number of inter-node messages
                this.taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
                this.taskMonitor.checkThreadId();
            }
        }

        @Override
        public void execute(Tuple tuple) {

            double[][] A = (double[][]) tuple.getValueByField("A");
            double[][] B = (double[][]) tuple.getValueByField("B");
            double[][] C = new double[A.length][B[0].length];
            if (A[0].length != B.length)
                LOG.error("Input matrix size unmatched");
            for (int i = 0; i < A.length; i++) {
                for (int j = 0; j < B[0].length; j++) {
                    for (int k = 0; k < B.length; k++) {
                        C[i][j] += A[i][k] * B[k][j];
                    }
                }
            }
            Values value = new Values();
            value.add(C);
            collector.emit(tuple, value);
            collector.ack(tuple);
        }
    }

    private static class aggregationBolt extends BaseRichBolt {

        private OutputCollector collector;
        private boolean enableAScheduler;
        private WorkerMonitor workerMonitor;
        private TaskMonitor taskMonitor;

        private aggregationBolt(boolean enableAScheduler) {
            this.enableAScheduler = enableAScheduler;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            if (this.enableAScheduler) {
                // this will create/configure the worker monitor once per worker
                this.workerMonitor = WorkerMonitor.getInstance();
                this.workerMonitor.setContextInfo(topologyContext);
                // this object is used in the emit/execute method to compute the number of inter-node messages
                this.taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
                this.taskMonitor.checkThreadId();
            }
        }

        @Override
        public void execute(Tuple tuple) {
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    private static class matrixSpout extends BaseRichSpout {

//        private final Logger LOG = Logger.getLogger(getClass());

        private SpoutOutputCollector collector;

        private long emitCount;

        private TaskMonitor taskMonitor;

        private WorkerMonitor workerMonitor;

        private boolean enableAScheduler;

        private int matrixSize;

        private matrixSpout(boolean enableAScheduler, int matrix_size) {
            this.enableAScheduler = enableAScheduler;
            this.matrixSize = matrix_size;
        }

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

            this.emitCount=0;
            this.collector = spoutOutputCollector;

            if (this.enableAScheduler) {
                // this will create/configure the worker monitor once per worker
                this.workerMonitor = WorkerMonitor.getInstance();
                this.workerMonitor.setContextInfo(topologyContext);
                // this object is used in the emit/execute method to compute the number of inter-node messages
                this.taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
                taskMonitor.checkThreadId();
            }
        }

        @Override
        public void nextTuple() {

            double[][] A = genMatrixArray(this.matrixSize);
            double[][] B = genMatrixArray(this.matrixSize);
            this.emitCount++;
            Values values = new Values();
            values.add(A);
            values.add(B);
            this.collector.emit(values,emitCount);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("A","B"));
        }
    }

    private static double[][] genMatrixArray(int size){

        double[][] ret = new double[size][size];
        for(int i=0;i<size;i++){
            for(int j=0;j<size;j++){
                double n;
                do {
                    n = rand.nextDouble();
                } while (n > Integer.MAX_VALUE);
                ret[i][j]=n;
            }
        }
        return ret;
    }
}
