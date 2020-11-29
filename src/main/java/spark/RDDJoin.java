package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class RDDJoin {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("RDDJoin").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<String, String>> list1 = Arrays.asList(
                new Tuple2<>("a", "a11"),
                new Tuple2<>("a", "a12"),
                new Tuple2<>("b", "b11"),
                new Tuple2<>("b", "b12"),
                new Tuple2<>("c", "c11"),
                new Tuple2<>("d", "d11")
        );
        JavaPairRDD<String, String> rdd1 = jsc.parallelizePairs(list1);
        List<Tuple2<String, String>> list2 = Arrays.asList(
                new Tuple2<>("a", "a21"),
                new Tuple2<>("a", "a22"),
                new Tuple2<>("b", "b21"),
                new Tuple2<>("c", "c21"),
                new Tuple2<>("c", "c22"),
                new Tuple2<>("d", "d22")
        );
        JavaPairRDD<String, String> rdd2 = jsc.parallelizePairs(list2);
        List<Tuple2<String, String>> list3 = Arrays.asList(
                new Tuple2<>("a", "a31"),
                new Tuple2<>("b", "b31"),
                new Tuple2<>("c", "c31"),
                new Tuple2<>("d", "d32")
        );
        JavaPairRDD<String, String> rdd3 = jsc.parallelizePairs(list3);
        List<Tuple2<String, String[]>> list4 = Arrays.asList(
                new Tuple2<>("a", new String[]{"a41", "a42"}),
                new Tuple2<>("b", new String[]{"b41"}),
                new Tuple2<>("c", new String[]{"c41"}),
                new Tuple2<>("d", new String[]{"d41"})
        );
        JavaPairRDD<String, String[]> rdd4 = jsc.parallelizePairs(list4);

        JavaPairRDD<String, Tuple2<String, Optional<String>>> rdd12 = rdd1.leftOuterJoin(rdd2);
        JavaPairRDD<String, Tuple2<Tuple2<String, Optional<String>>, Optional<String>>> rdd123 = rdd1.leftOuterJoin(rdd2).leftOuterJoin(rdd3);
        JavaPairRDD<String, Tuple2<Tuple2<Tuple2<String, Optional<String>>, Optional<String>>, Optional<String[]>>> rdd1234 = rdd1.leftOuterJoin(rdd2).leftOuterJoin(rdd3).leftOuterJoin(rdd4);
//        rdd12.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Optional<String>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<String, Optional<String>>> input) throws Exception {
//                System.out.println(input._1 + " : " + input._2._1 + "~" + input._2._2);
//            }
//        });
//        rdd123.foreach(new VoidFunction<Tuple2<String, Tuple2<Tuple2<String, Optional<String>>, Optional<String>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Tuple2<String, Optional<String>>, Optional<String>>> input) throws Exception {
//                System.out.println(input._1 + " : " + input._2);
//            }
//        });

//        rdd1234.foreach(new VoidFunction<Tuple2<String, Tuple2<Tuple2<Tuple2<String, Optional<String>>, Optional<String>>, Optional<String[]>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Tuple2<Tuple2<String, Optional<String>>, Optional<String>>, Optional<String[]>>> input) throws Exception {
//                System.out.println(input._1 + " : " + input._2);
//            }
//        });
        JavaPairRDD<String, Iterable<String>> groupByKey = rdd1.groupByKey();
        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> input) throws Exception {
                System.out.println(input._1 + " : " + input._2);
            }
        });

    }
}
