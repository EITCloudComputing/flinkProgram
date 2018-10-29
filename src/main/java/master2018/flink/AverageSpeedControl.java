package master2018.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageSpeedControl {

    public static void main(String[] args) throws Exception {

        System.out.println("Starting AverageSpeedControl...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFolderPath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);

        source.writeAsText(outFolderPath + "avgspeedfines.txt");

        try {
            env.execute("AverageSpeedControl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
