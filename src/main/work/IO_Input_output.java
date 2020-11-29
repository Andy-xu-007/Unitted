import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class IO_Input_output {
    public static void main(String[] args) throws IOException {
        File f1 = new File("text.txt");
        if (!f1.isFile())
            f1.createNewFile();
        FileWriter writer = new FileWriter("text.txt", true);
        writer.write("andy" + "\r\n");
        writer.write("f" + "\r\n");
        writer.close();
    }
}
