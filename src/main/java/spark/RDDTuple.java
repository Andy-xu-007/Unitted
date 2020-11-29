package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class
RDDTuple {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Random rand = new Random();
        String[] split = "andy,athena,cammie,bill,kenney,jesse".split(",");

        List<Character> number1 = Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'c', 'd', 'e', 'c', 'h');
        JavaRDD<Character> rdd1 = sc.parallelize(number1);
        JavaPairRDD<Character, String[]> char2Tuple = rdd1.mapToPair(new PairFunction<Character, Character, String[]>() {
            @Override
            public Tuple2<Character, String[]> call(Character ch) throws Exception {
                String[] strs = new String[3];
                strs[0] = split[rand.nextInt(6)];
                strs[1] = split[rand.nextInt(6)];
                strs[2] = split[rand.nextInt(6)];
                return new Tuple2<>(ch, strs);
            }
        });

        JavaRDD<String[]> rdd3 = char2Tuple.values();
        rdd3.foreach(new VoidFunction<String[]>() {
            @Override
            public void call(String[] strings) throws Exception {
                StringBuilder str = new StringBuilder();
                for (String str1 : strings){
                    str.append(str1 + ", ");
                }
                System.out.println(str.toString());
            }
        });

        JavaPairRDD<String, Iterable<String[]>> rdd2 = rdd3.groupBy(new Function<String[], String>() {
            @Override
            public String call(String[] v1) throws Exception {
                return v1[0] + "|" + v1[1];
            }
        });

        rdd2.foreach(new VoidFunction<Tuple2<String, Iterable<String[]>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String[]>> strs) throws Exception {
                Iterator<String[]> iterator = strs._2.iterator();
                StringBuilder str = new StringBuilder();
                while (iterator.hasNext()){
                    for (String str1 : iterator.next()){
                        str.append(str1 + ", ");
                    }
                }
                System.out.println(strs._1 + " : " + str);

            }
        });

    }
}
