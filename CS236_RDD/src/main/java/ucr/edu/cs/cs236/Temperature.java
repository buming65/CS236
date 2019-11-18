package ucr.edu.cs.cs236;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Temperature {
    public static void main(String[] args){
        if(args.length != 3){
            System.out.println("ERROR: The length of configurations is not right.");
            System.out.println("Please input the path of the Locations File, the Recordings Files and the Output Folder");
            System.out.println("EXITING");
            System.exit(1);
        }
        String path_location = args[0];
        String path_recording = args[1];
        String path_output = args[2];


        SparkConf conf = new SparkConf().setAppName("Temperature").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> location = sc.textFile(path_location);
        String country = new String("US");
        JavaRDD<String> in_USA = location.filter(
                s -> s.split("\t")[3].equals(country));
        System.out.println(in_USA.collect());
//        JavaRDD<Integer> lines = location.map((s) ->s.length());

    }
}
