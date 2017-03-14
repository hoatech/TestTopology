package edu.fudan.storm.stormtest.topology;

import edu.fudan.storm.LoadMonitor.TaskMonitor;
import edu.fudan.storm.LoadMonitor.WorkerMonitor;
import edu.fudan.storm.metrichook.SchedulingMetricsToZookeeperWriter;
import edu.fudan.storm.stormtest.utils.TestUtil;
import edu.fudan.storm.tools.metrics.BasicMetricsCollector;
import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.*;
import jcuda.runtime.dim3;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static jcuda.driver.JCudaDriver.*;
import static jcuda.driver.JCudaDriver.cuMemFree;
import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;

/**
 * Created by ybwang on 3/14/17.
 */
public class GPUMatrixMultiplyTopology {

    private static Logger LOG = Logger.getLogger(GPUMatrixMultiplyTopology.class);
    private static Random rand = new Random();

    @Option(name="--topologyname", aliases={"-name"}, metaVar="topology name",
            usage="name of the topology")
    private String topoName = "GPUMatrixMultiplyTopology";

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
    private int matrix_size=32;

    public static void main(String[] args) {
        new GPUMatrixMultiplyTopology().realMain(args);
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
        String resultPath = TestUtil.result_path + "/GPUMatrixMultiplyTopology/";
        new File(resultPath).mkdirs();
        conf.put("metrics.path", resultPath);
        conf.put("metrics.time", this.runtime);
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("matrix-spout", new matrixSpout(this.enableHScheduler, this.matrix_size), this.parallelism);
        b.setBolt("multiply-bolt", new mulBolt(this.enableHScheduler), this.parallelism).shuffleGrouping("matrix-spout");
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

    public static class mulBolt extends BaseRichBolt{

        private OutputCollector collector;
        private boolean enableAScheduler;
        private WorkerMonitor workerMonitor;
        private TaskMonitor taskMonitor;
        private CUfunction function;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("result"));
        }

        mulBolt(boolean enableAScheduler) {
            this.enableAScheduler = enableAScheduler;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            // Enable exceptions and omit all subsequent error checks
            JCudaDriver.setExceptionsEnabled(true);

            // Initialize the driver and create a context for the first device.
            cuInit(0);
            CUdevice device = new CUdevice();
            cuDeviceGet(device, 0);
            CUcontext context = new CUcontext();
            cuCtxCreate(context, 2, device);
            // Create the PTX file by calling the NVCC
            String ptxFileName = null;
            try {
                ptxFileName = preparePtxFile(TestUtil.cu_file_path+"matrixMultiplicationKernel.cu");
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Load the ptx file.
            CUmodule module = new CUmodule();
            cuModuleLoad(module, ptxFileName);
            // Obtain a function pointer to the "sampleKernel" function.
            this.function = new CUfunction();
            cuModuleGetFunction(function, module, "matrixMultiplicationKernel");

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

            double[] A = (double[]) tuple.getValueByField("A");
            double[] B = (double[]) tuple.getValueByField("B");
            double[] C = null;
            try {
                C = matrixMultiplication(A, B, (int)Math.sqrt(A.length));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(C!=null) {
                Values values = new Values();
                values.add(C);
                collector.emit(tuple, values);
            }
            collector.ack(tuple);
        }
        private double[] matrixMultiplication(double[] A, double[] B, int size) throws IOException {

            // Allocate the device input data, and copy the
            // host input data to the device
            CUdeviceptr deviceInputA = new CUdeviceptr();
            cuMemAlloc(deviceInputA, size*size * Sizeof.DOUBLE);
            cuMemcpyHtoD(deviceInputA, Pointer.to(A),
                    size*size * Sizeof.DOUBLE);
            CUdeviceptr deviceInputB = new CUdeviceptr();
            cuMemAlloc(deviceInputB, size*size * Sizeof.DOUBLE);
            cuMemcpyHtoD(deviceInputB, Pointer.to(B),
                    size*size * Sizeof.DOUBLE);

            // Allocate device output memory
            CUdeviceptr deviceOutput = new CUdeviceptr();
            cuMemAlloc(deviceOutput, size*size * Sizeof.DOUBLE);

            Pointer kernelParameters = Pointer.to(
                    Pointer.to(deviceInputA),
                    Pointer.to(deviceInputB),
                    Pointer.to(deviceOutput),
                    Pointer.to(new int[]{size})
            );
            // declare the number of blocks per grid and the number of threads per block
            // use 1 to 512 threads per block
            dim3 threadsPerBlock = new dim3(size, size, 1);
            dim3 blocksPerGrid=new dim3(1, 1, 1);
            if(size*size>32){
                threadsPerBlock.x = 32;
                threadsPerBlock.y = 32;
                blocksPerGrid.x = (int)Math.ceil((double)size/(double)threadsPerBlock.x);
                blocksPerGrid.y = (int)Math.ceil((double)size/(double)threadsPerBlock.y);
            }
            cuLaunchKernel(this.function,
                    blocksPerGrid.x, blocksPerGrid.y, 1,           // Grid dimension
                    threadsPerBlock.x, threadsPerBlock.y, 1,  // Block dimension
                    0, null,           // Shared memory size and stream
                    kernelParameters, null // Kernel- and extra parameters
            );
            cuCtxSynchronize();
            // Allocate host output memory and copy the device output
            // to the host.
            double hostOutput[] = new double[size*size];
            cuMemcpyDtoH(Pointer.to(hostOutput), deviceOutput,
                    size*size * Sizeof.DOUBLE);
            // Clean up.
            cuMemFree(deviceInputA);
            cuMemFree(deviceInputB);
            cuMemFree(deviceOutput);
            return hostOutput;
        }
        private static String preparePtxFile(String cuFileName) throws IOException {
            int endIndex = cuFileName.lastIndexOf('.');
            if (endIndex == -1) {
                endIndex = cuFileName.length() - 1;
            }
            String ptxFileName = cuFileName.substring(0, endIndex + 1) + "ptx";
            File ptxFile = new File(ptxFileName);
            if (ptxFile.exists()) {
                return ptxFileName;
            }

            File cuFile = new File(cuFileName);
            if (!cuFile.exists()) {
                throw new IOException("Input file not found: " + cuFileName);
            }
            String modelString = "-m" + System.getProperty("sun.arch.data.model");
            String command =
                    "nvcc " + modelString + " -ptx " +
                            cuFile.getPath() + " -o " + ptxFileName;

            LOG.info("Executing\n" + command);
            Process process = Runtime.getRuntime().exec(command);

            String errorMessage =
                    new String(toByteArray(process.getErrorStream()));
            String outputMessage =
                    new String(toByteArray(process.getInputStream()));
            int exitValue = 0;
            try {
                exitValue = process.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting for nvcc output");
            }

            if (exitValue != 0) {
                LOG.info("nvcc process exitValue " + exitValue);
                LOG.info("errorMessage:\n" + errorMessage);
                LOG.info("outputMessage:\n" + outputMessage);
                throw new IOException(
                        "Could not create .ptx file: " + errorMessage);
            }
            LOG.info("Finished creating PTX file");
            return ptxFileName;
        }
        private static byte[] toByteArray(InputStream inputStream)
                throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte buffer[] = new byte[8192];
            while (true) {
                int read = inputStream.read(buffer);
                if (read == -1) {
                    break;
                }
                baos.write(buffer, 0, read);
            }
            return baos.toByteArray();
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
            double[] A = genMatrixArray(this.matrixSize);
            double[] B = genMatrixArray(this.matrixSize);
            this.emitCount++;
            Values values = new Values();
            values.add(A);
            values.add(B);
            this.collector.emit(values, emitCount);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("A","B"));
        }
    }
    private static double[] genMatrixArray(int size){

        double[] ret = new double[size*size];
        for(int i=0;i<ret.length;i++){
            double n;
            do {
                n = rand.nextDouble();
            } while (n > Integer.MAX_VALUE);
            ret[i]=n;
        }
        return ret;
    }
}
