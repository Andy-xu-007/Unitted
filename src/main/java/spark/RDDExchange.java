package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.catalyst.expressions.Rand;
import scala.Tuple2;

import java.util.*;

public class RDDExchange {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Random rand = new Random();

        List<Character> number1 = Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g');
        JavaRDD<Character> rdd1 = sc.parallelize(number1);

        List<String> number2 = Arrays.asList("hello same you","hello some me","hello same world", "hello some world");
        JavaRDD<String> rdd2 = sc.parallelize(number2).repartition(2);

        List<Tuple2<String, Integer>> number3 = Arrays.asList(
                new Tuple2<String, Integer>("class1", 87),
                new Tuple2<String, Integer>("class2", 91),
                new Tuple2<String, Integer>("class1", 97),
                new Tuple2<String, Integer>("class2", 89),
                new Tuple2<String, Integer>("class1", 83),
                new Tuple2<String, Integer>("class2", 85),
                new Tuple2<String, Integer>("class1", 87),
                new Tuple2<String, Integer>("class2", 91)
        );
        JavaPairRDD<String, Integer> rdd3 = sc.parallelizePairs(number3).repartition(2);

        List<Tuple2<String, Integer>> number4 = Arrays.asList(
                new Tuple2<String, Integer>("e", 5),
                new Tuple2<String, Integer>("c", 3),
                new Tuple2<String, Integer>("d", 4),
                new Tuple2<String, Integer>("c", 2),
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("b", 6)
        );
        JavaPairRDD<String, Integer> rdd4 = sc.parallelizePairs(number4);

        // 1, mapToPair
        JavaPairRDD<Integer, Character> rdd1MapToPair = rdd1.mapToPair(new PairFunction<Character, Integer, Character>() {
            @Override
            public Tuple2<Integer, Character> call(Character str) throws Exception {
                return new Tuple2<>(rand.nextInt(10), str);
            }
        });
        // map
        JavaRDD<Tuple2<Integer, Character>> rdd1Map = rdd1.map(new Function<Character, Tuple2<Integer, Character>>() {
            @Override
            public Tuple2<Integer,Character> call(Character str) throws Exception {
                return new Tuple2<>(rand.nextInt(10), str);
            }
        });

        // 2, filter
        rdd1Map.filter(new Function<Tuple2<Integer, Character>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Character> f) throws Exception {
                return f._1 > 5;
            }
        });
        rdd1MapToPair.filter(new Function<Tuple2<Integer, Character>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Character> f) throws Exception {
                return f._1 > 5;
            }
        });

        // 3, flatMap
        JavaRDD<String> rdd2FlatMap = rdd2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String str) throws Exception {
                return Arrays.asList(str.split(" ")).iterator();
            }
        });
        JavaRDD<Tuple2<String, Integer>> rdd2FlatMap_2 = rdd2.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String str) throws Exception {
                String[] s = str.split(" ");
                List<Tuple2<String, Integer>> ss = new ArrayList<>();
                ss.add(new Tuple2(rand.nextInt(10), s[0]));
                ss.add(new Tuple2(rand.nextInt(10), s[2]));
                return ss.iterator();
            }
        });

        // 4, sortByKey
        JavaPairRDD<Integer, Character> rdd1MapToPairSortResult = rdd1MapToPair.sortByKey();

        // 5, collectAsMap
        Map<String, Integer> rdd2Map = rdd3.collectAsMap();

        // 6, combineByKey
        JavaRDD<Tuple2<String, Float>> rdd3CombineByKey = rdd3.combineByKey(new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                return new Tuple2<>(v1, 1);
            }
        }, new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Integer v2) throws Exception {
                return new Tuple2<>(v1._1 + v2, v1._2 + 1);
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
            }
        }).map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Float>>() {
            @Override
            public Tuple2<String, Float> call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return new Tuple2<>(v1._1, (float) (v1._2._1 / v1._2._2));
            }
        });

        // 7, aggregateByKey
        JavaPairRDD<String, Integer> rdd3AggregateByKey = rdd3.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Math.max(v1, v2);
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Math.max(v1, v2);
            }
        });


        List<Character> lookup = rdd1MapToPair.lookup(3);
        for (char i : lookup) {
            System.out.println(i);
        }


//        rdd2FlatMap_2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> st) throws Exception {
//                System.out.println(st);
//            }
//        });
//        System.out.println("~~~~~~~~~~~~~~~");
//        rdd2FlatMap_2.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//            @Override
//            public void call(Iterator<Tuple2<String, Integer>> st) throws Exception {
//                while (st.hasNext()) {
//                    System.out.println(st.next());
//                }
//            }
//        });
//        System.out.println(rdd2FlatMap_2.partitions().size());
//        rdd3CombineByKey.foreach(new VoidFunction<Tuple2<String, Float>>() {
//            @Override
//            public void call(Tuple2<String, Float> stringDoubleTuple2) throws Exception {
//                System.out.println(stringDoubleTuple2);
//            }
//        });
//        rdd3AggregateByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2);
//            }
//        });
        sc.stop();
    }
}
