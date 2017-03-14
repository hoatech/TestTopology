import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.*;
import jcuda.runtime.dim3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static jcuda.driver.JCudaDriver.*;
import static jcuda.driver.JCudaDriver.cuMemFree;

/**
 * Created by ybwang on 3/14/17.
 */
public class cudaTest {

    public static void main(String[] args) throws IOException {
        int size = 128;
        double[] A = new double[size*size];
        double[] B = new double[size*size];
        for(int i=0;i<size*size;i++) {
            A[i] = (double) i;
            B[i]=(double)i;
        }
        double[] C = new cudaTest().matrixMultiplication(A, B, size);
        System.out.println(Arrays.toString(C));
        double[][] A1 = new double[size][size];
        double[][] B1 = new double[size][size];
        double[][] C1 = new double[size][size];
        for(int i=0;i<size;i++){
            for(int j=0;j<size;j++){
                A1[i][j]=i*size+j;
                B1[i][j]=i*size+j;
            }
        }
        for (int i = 0; i < A1.length; i++) {
            for (int j = 0; j < B1[0].length; j++) {
                for (int k = 0; k < B1.length; k++) {
                    C1[i][j] += A1[i][k] * B1[k][j];
                }
            }
        }
        System.out.println(Arrays.deepToString(C1));

    }
    private double[] matrixMultiplication(double[] A, double[] B, int size) throws IOException {

        // Enable exceptions and omit all subsequent error checks
        JCudaDriver.setExceptionsEnabled(true);

        // Initialize the driver and create a context for the first device.
        cuInit(0);
        CUdevice device = new CUdevice();
        cuDeviceGet(device, 0);
        CUcontext context = new CUcontext();
        cuCtxCreate(context, 2, device);
        // Create the PTX file by calling the NVCC
        String ptxFileName = preparePtxFile("src/main/resources/matrixMultiplicationKernel.cu");
        // Load the ptx file.
        CUmodule module = new CUmodule();
        cuModuleLoad(module, ptxFileName);
        // Obtain a function pointer to the "sampleKernel" function.
        CUfunction function = new CUfunction();
        cuModuleGetFunction(function, module, "matrixMultiplicationKernel");

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
        cuLaunchKernel(function,
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

        System.out.println("Executing\n" + command);
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
            System.out.println("nvcc process exitValue " + exitValue);
            System.out.println("errorMessage:\n" + errorMessage);
            System.out.println("outputMessage:\n" + outputMessage);
            throw new IOException(
                    "Could not create .ptx file: " + errorMessage);
        }

        System.out.println("Finished creating PTX file");
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
