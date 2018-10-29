package master2018.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AccidentReporter {

    public static void main(String[] args) throws Exception {

        System.out.println("Starting AccidentReporter...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFolderPath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);

        source.writeAsText(outFolderPath + "accidents.txt");

        try {
            env.execute("AccidentReporter");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
