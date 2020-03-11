/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//code reference https://spark.apache.org/docs/2.2.0/ml-classification-regression.html
package com.mycompany.bigdataassignment2_k_means_p2_data2;

/**
 *
 * @author Ray
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
public class KmeansSolution {
    public static void main(String[] args){
        // Load and parse data
        SparkConf sparkConf = new SparkConf().setAppName("KmeansSolution");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String path = "ruixinhhw2input/complaints_adjusted.csv";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
          String[] sarray = s.split("\n|,");
          double[] values = new double[sarray.length];
          for (int i = 0; i < sarray.length; i++) {
            values[i] = Double.parseDouble(sarray[i]);
          }
          return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = 5;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
          System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        // Save and load model
        //clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
        //KMeansModel sameModel = KMeansModel.load(jsc.sc(),"target/org/apache/spark/JavaKMeansExample/KMeansModel");
    }
}
