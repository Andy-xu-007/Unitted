import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext("local", "keyValueRDDTest", conf);

        List<Tuple2<Integer, Integer>> list = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 2),
                new Tuple2<Integer, Integer>(3, 4),
                new Tuple2<Integer, Integer>(3, 6));
        List<Tuple2<Integer, Integer>> list2 = Arrays.asList(
                new Tuple2<Integer, Integer>(3, 9)
        );

        JavaPairRDD<Integer, Integer> rdd = jsc.parallelizePairs(list);
        JavaPairRDD<Integer, Integer> rdd2 = jsc.parallelizePairs(list2);

        JavaPairRDD<Integer, Integer> JPRDD_ReduceByKey = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1;
            }
        });

        System.out.println(JPRDD_ReduceByKey.collect());


        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(1);
        list1.add(2);
        list1.add(3);
        list1.add(4);
        list1.add(3);

        JavaRDD<Integer> rdd3 = jsc.parallelize(list1);
        rdd3.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer v) throws Exception {
                return new Tuple2<Integer, Integer>(v, 1);
            }
        });
    }
}
