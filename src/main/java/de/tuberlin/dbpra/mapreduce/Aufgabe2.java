package de.tuberlin.dbpra.mapreduce;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Aufgabe2 {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: <input> <output>");
        }
        SparkConf conf = new SparkConf().setAppName("DBPRA").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRdd = sc.textFile(args[0]);
        //JavaPairRDD<String, Double> outputRdd = null;

        JavaPairRDD<String, Integer> artikelAnzahl = inputRdd
                .mapToPair(new getArtikelAnzahl());
                //.reduceByKey((a, b) -> a + b).sortByKey();
        JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = artikelAnzahl.mapValues(value -> new Tuple2<Integer, Integer>(value,1));
        //add values by reduceByKey
        JavaPairRDD<String, Tuple2<Integer, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        //calculate average
        JavaPairRDD<String, Double> outputRdd = reducedCount.mapToPair(getAverageByKey).sortByKey();
        // use this method to write the RDD into a file
        writeRDD(outputRdd, args[1]);
        sc.stop();
    }

    static class getArtikelAnzahl implements PairFunction<String, String, Integer> {
        @Override
        public Tuple2<String, Integer> call(String input) throws Exception {
            String[] splitted = input.split("\\|");
            Integer anzahl = Integer.valueOf(splitted[4]);
            String artikelID = splitted[1];
            return new Tuple2<String, Integer>(artikelID, anzahl);}
        }

    private static PairFunction<Tuple2<String, Tuple2<Integer, Integer>>,String,Double> getAverageByKey = (tuple) -> {
        Tuple2<Integer, Integer> val = tuple._2;
        Double total = Double.valueOf(val._1);
        Double count = Double.valueOf(val._2);
        Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, total / count);
        return averagePair;
    };

    public static void writeRDD(JavaPairRDD<String, Double> writeable, String outputFile) throws IOException {
        // collect RDD for writing
        PrintWriter pw = new PrintWriter(new FileWriter(outputFile));
        for (Tuple2<String, Double> line : writeable.collect()) {
            pw.println(line._1 + "|" + line._2);
        }
        pw.close();
    }

}
