import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

public class JsonFileFinder {
    public static List<Path> getAllJsonFiles(String directory) throws IOException {
        List<Path> jsonFiles = new ArrayList<>();
        Path startPath = Paths.get(directory);

        Files.walkFileTree(startPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (file.toString().endsWith(".json")) {
                    jsonFiles.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return jsonFiles;
    }

    public static void main(String[] args) {
        String directory = "/path/to/your/directory";
        try {
            List<Path> jsonFiles = getAllJsonFiles(directory);
            for (Path jsonFile : jsonFiles) {
                System.out.println(jsonFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
