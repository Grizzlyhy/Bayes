package hust.grizzlyhy;

import java.io.File;
import java.util.Arrays;

import static java.lang.System.exit;

public class preDatabase {
    /**
     *
     *
     * @param inputDir
     * @param outputDir
     */
    public static void shuffle(String inputDir, String outputDir) {
        File inputDirPath = new File(inputDir);
        if (!inputDirPath.isDirectory()) {
            exit(1001);
        }
        File[] inputDirs = inputDirPath.listFiles();

        File trainPath = new File(outputDir + "\\train");
        if (!trainPath.exists()) {
            trainPath.mkdirs();
        }
        File testPath = new File(outputDir + "\\test");
        if (!testPath.exists()) {
            testPath.mkdirs();
        }
        for(int i=0;i<inputDirs.length;i++)
            selletTestSet(inputDir,outputDir,inputDirs[i].getName());
    }

    public static void selletTestSet(String inputDir, String outputDir, String type) {
        File typeDirPath = new File(inputDir + "\\" + type);
        File[] typeFiles = typeDirPath.listFiles();

        String trainPath = outputDir + "\\train";
        String testPath = outputDir + "\\test";

        Arrays.stream(typeFiles).unordered();
        int i = 0;
        int flag = typeFiles.length * 7 / 10;
        for (File f : typeFiles) {
            i++;
            if (i <= flag) {
                f.renameTo(new File(trainPath, type +"_"+ i + ".txt"));
            } else {
                f.renameTo(new File(testPath, type +"_"+ i + ".txt"));
            }
        }

    }

    public static void main(String[] args) {
        shuffle("G:\\Bayes\\Bayes\\data", "G:\\Bayes\\Bayes\\bayes_data");
    }
}
