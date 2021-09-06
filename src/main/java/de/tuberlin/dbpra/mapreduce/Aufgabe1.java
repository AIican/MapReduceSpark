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
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

public class Aufgabe1 {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: <input> <output>");
        }
        SparkConf conf = new SparkConf().setAppName("DBPRA").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRdd = sc.textFile(args[0]);
        //JavaPairRDD<String, Integer> outputRdd = null;

        JavaPairRDD<String, Integer> outputRdd = inputRdd.filter(k -> k.contains("RAIL"))
                .mapToPair(new getAnzahl())
                .reduceByKey((a, b) -> a + b).sortByKey();

        // use this method to write the RDD into a file
        writeRDD(outputRdd, args[1]);
        sc.stop();
    }

    static class getAnzahl implements PairFunction<String, String, Integer> {
        @Override
        public Tuple2<String, Integer> call(String input) throws Exception {
            String[] splitted = input.split("\\|");
            Integer anzahl = Integer.valueOf(splitted[4]);
            String versanddatum = splitted[10].substring(0,7);
            return new Tuple2<String, Integer>(versanddatum, anzahl);
        }
    }

    public static void writeRDD(JavaPairRDD<String, Integer> writeable, String outputFile) throws IOException {
        // collect RDD for writing
        PrintWriter pw = new PrintWriter(new FileWriter(outputFile));
        for (Tuple2<String, Integer> line : writeable.collect()) {
            pw.println(line._1 + "|" + line._2);
        }
        pw.close();
    }

}
