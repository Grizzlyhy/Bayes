package hust.grizzlyhy;

public class FilePathBean {


    private static String LOCAL_DATA_BEFORE="G:\\Bayes\\Bayes\\data";
    private static String LOCAL_BASE_PATH="G:\\Bayes\\Bayes\\bayes_data";
    private static String LOCAL_TRAIN_PATH=LOCAL_BASE_PATH+"\\train";
    private static String LOCAL_TEST_PATH=LOCAL_BASE_PATH+"\\test";

    private static String BASE_PATH="/input/bayes";
    private static String TRAIN_DATA_PATH = BASE_PATH+"/train";
    private static String TEST_DATA_PATH = BASE_PATH+"/test";

    private static String OUTPUT_BATH_PATH="/output/bayes";
    private static String OUTPUT_PRIOR_PATH=OUTPUT_BATH_PATH+"/TypeNum";
    private static String OUTPUT_CONDI_PATH=OUTPUT_BATH_PATH+"/ConditionNum";
    private static String OUTPUT_TESTPRE_PATH=OUTPUT_BATH_PATH+"/TestPre";

    private static String OUTPUT_TEST_PATH=OUTPUT_BATH_PATH+"/Test";

    public static String getOutputTestPath() {
        return OUTPUT_TEST_PATH;
    }

    public static String getOutputTestprePath() {
        return OUTPUT_TESTPRE_PATH;
    }

    public static String getOutputCondiPath() {
        return OUTPUT_CONDI_PATH;
    }

    public static String getLocalTestPath() {
        return LOCAL_TEST_PATH;
    }

    public static String getBasePath() {
        return BASE_PATH;
    }

    public static String getTrainDataPath() {
        return TRAIN_DATA_PATH;
    }

    public static String getTestDataPath() {
        return TEST_DATA_PATH;
    }

    public static String getLocalBasePath() {
        return LOCAL_BASE_PATH;
    }

    public static String getLocalTrainPath() {
        return LOCAL_TRAIN_PATH;
    }

    public static String getLocalDataBefore() {
        return LOCAL_DATA_BEFORE;
    }

    public static String getOutputBathPath() {
        return OUTPUT_BATH_PATH;
    }

    public static String getOutputPriorPath() {
        return OUTPUT_PRIOR_PATH;
    }
}
