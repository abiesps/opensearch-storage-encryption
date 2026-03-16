import java.util.Random;
public class ComputeSeed {
    public static void main(String[] args) {
        Random r = new Random(0xCAFEBABE_DEADBEEFL);
        int blockSize = 8192;
        int sequentialReadNumBytes = r.nextInt(0, blockSize - 3);
        System.out.println("sequentialReadNumBytes = " + sequentialReadNumBytes);
        int numBlocks = (1048576 + 3 + blockSize - 1) / blockSize; // 1MB + 3 bytes
        System.out.println("numBlocks = " + numBlocks);
        System.out.println("total bytes per op = " + (long)numBlocks * sequentialReadNumBytes);
    }
}
