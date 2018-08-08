import java.nio.file.Files;
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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
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

    /** The default size of a file buffer. */
    private static final int DEFAULT_BUFFER_SIZE = 16384;

    /**
     * The maximum size of a file buffer array. Eight bytes smaller than {@link Integer.MAX_VALUE}, since some VMs
     * reserve header words in arrays.
     */
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    /** The maximum initial buffer size. */
    private static final int MAX_INITIAL_BUFFER_SIZE = 16 * 1024 * 1024;

    /**
     * Read all the bytes in an {@link InputStream}.
     * 
     * @param inputStream
     *            The {@link InputStream}.
     * @param fileSizeHint
     *            The file size, if known, otherwise -1L.
     * @return The contents of the {@link InputStream} as an Entry consisting of the byte array and number of bytes
     *         used in the array..
     * @throws IOException
     *             If the contents could not be read.
     */
    private static SimpleEntry<byte[], Integer> readAllBytes(
                final InputStream inputStream, final long fileSizeHint) throws IOException {
        if (fileSizeHint > MAX_BUFFER_SIZE) {
            throw new IOException("InputStream is too large to read");
        }
        final int bufferSize = fileSizeHint < 1L
                // If fileSizeHint is unknown, use default buffer size 
                ? DEFAULT_BUFFER_SIZE
                // fileSizeHint is just a hint -- limit the max allocated buffer size, so that invalid ZipEntry
                // lengths do not become a memory allocation attack vector
                : Math.min((int) fileSizeHint, MAX_INITIAL_BUFFER_SIZE);
        byte[] buf = new byte[bufferSize];

        int bufLength = buf.length;
        int totBytesRead = 0;
        for (int bytesRead;;) {
            // Fill buffer -- may fill more or fewer bytes than buffer size
            while ((bytesRead = inputStream.read(buf, totBytesRead, bufLength - totBytesRead)) > 0) {
                totBytesRead += bytesRead;
            }
            if (bytesRead < 0) {
                // Reached end of stream
                break;
            }
            // bytesRead == 0 => grow buffer, avoiding overflow
            if (bufLength <= MAX_BUFFER_SIZE - bufLength) {
                bufLength = bufLength << 1;
            } else {
                if (bufLength == MAX_BUFFER_SIZE) {
                    throw new IOException("InputStream too large to read");
                }
                bufLength = MAX_BUFFER_SIZE;
            }
            buf = Arrays.copyOf(buf, bufLength);
        }
        // Return buffer and number of bytes read
        return new SimpleEntry<>((bufLength == totBytesRead) ? buf : Arrays.copyOf(buf, totBytesRead),
                totBytesRead);
    }

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
                            try (InputStream is = Files.newInputStream(f.toPath())) {
                                readAllBytes(is, f.length());
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
