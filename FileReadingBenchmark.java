import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FileReadingBenchmark {
    /**
     * Total number of bytes -- should be around 2x RAM size to prevent caching (and you need this much free disk
     * space).
     */
    private static final int TOT_BYTES = 102_400_000;
    private static final int FILES_PER_DIR = 1000;

    public static void main(String[] args) throws IOException {
        System.out.println("Filesize\tNumFiles\t|\tInputStream1\tInputStream2\tInputStream4\tInputStream8\t|\t"
                + "FileChannel1\tFileChannel2\tFileChannel4\tFileChannel8");
        ExecutorService threadPools[] = new ExecutorService[] { Executors.newFixedThreadPool(1),
                Executors.newFixedThreadPool(2), Executors.newFixedThreadPool(4), Executors.newFixedThreadPool(8) };
        try {
            for (int numFiles = 100; numFiles <= 102400; numFiles *= 2) {
                int fileSize = (int) Math.ceil((float) TOT_BYTES / (float) numFiles);
                System.out.print(fileSize + "\t" + numFiles + "\t|");
                List<File> dirsAndFilesToDelete = new ArrayList<>();
                List<File> filesToRead = new ArrayList<>();
                Random random = new Random();
                try {
                    // Create temporary dir
                    Path tempDirPath = Files.createTempDirectory("benchmark");
                    final File tempDir = tempDirPath.toFile();
                    tempDir.deleteOnExit();
                    dirsAndFilesToDelete.add(tempDir);

                    // Create temporary files
                    File dir = null;
                    for (int i = 0; i < numFiles; i++) {
                        if (i % FILES_PER_DIR == 0) {
                            dir = new File(tempDir, "" + (i / FILES_PER_DIR));
                            if (!dir.mkdir()) {
                                throw new IOException("Could not make dir " + dir);
                            }
                            dir.deleteOnExit();
                            dirsAndFilesToDelete.add(dir);
                        }
                        byte[] bytes = new byte[fileSize];
                        random.nextBytes(bytes);
                        File file = new File(dir, "" + i);
                        try (FileOutputStream out = new FileOutputStream(file)) {
                            out.write(bytes);
                        }
                        dirsAndFilesToDelete.add(file);
                        filesToRead.add(file);
                    }

                    // Try reading files using InputStream
                    for (ExecutorService threadPool : threadPools) {
                        long t1 = System.nanoTime();
                        filesToRead.stream().map(f -> threadPool.submit(() -> {
                            try (InputStream is = new FileInputStream(f)) {
                                is.readAllBytes();
                            }
                            return null;
                        })).forEach(f -> {
                            // Barrier
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        });
                        System.out.print("\t" + String.format("%.4f", (System.nanoTime() - t1) * 1e-9));
                    }
                    System.out.print("\t|");

                    // Try reading files using memory-mapped ByteBuffer
                    for (ExecutorService threadPool : threadPools) {
                        long t1 = System.nanoTime();
                        filesToRead.stream().map(f -> threadPool.submit(() -> {
                            try (RandomAccessFile raf = new RandomAccessFile(f, "r");
                                    FileChannel fc = raf.getChannel()) {
                                final MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
                                buffer.load();
                                final byte[] bytes = new byte[buffer.remaining()];
                                buffer.get(bytes);
                            }
                            return null;
                        })).forEach(f -> {
                            // Barrier
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        });
                        System.out.print("\t" + String.format("%.4f", (System.nanoTime() - t1) * 1e-9));
                    }

                } finally {
                    // Remove temporary files
                    for (int i = dirsAndFilesToDelete.size() - 1; i >= 0; --i) {
                        File f = dirsAndFilesToDelete.get(i);
                        if (!f.delete()) {
                            System.err.println("Could not delete " + f);
                        }
                    }
                }
                System.out.println();
            }
        } finally {
            // Shut down thread pools
            for (ExecutorService threadPool : threadPools) {
                threadPool.shutdown();
                try {
                    threadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
                threadPool.shutdownNow();
            }
        }
        System.out.println("\nFinished.");
    }

}
