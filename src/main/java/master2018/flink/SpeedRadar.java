package master2018.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SpeedRadar {

    public static void main(String[] args) throws Exception {

        System.out.println("Starting SpeedRadar...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFolderPath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);

        source.writeAsText(outFolderPath + "speedfines.txt");

        try {
            env.execute("SpeedRadar");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
