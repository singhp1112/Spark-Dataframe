package com.prakash.spark.dataframe;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.prakash.spark.models.Movie;

/**
 * SimpleGroupByUsingDataFrame - Test performance of group by operation on data
 * using spark 1.6 Dataframe APIs. Data set -
 * https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset. Data is duplicated
 * multiple times to make it larger (approx. > 1M) for performance testing. In
 * this test only reduce by key operation is performed to avoid any RDD specific
 * optimization. Goal is to find total number of faces per language from entire
 * movie dataset.
 */
public class SimpleGroupByUsingDataFrame {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Group By Test Spark Dataframe 1.6")
				.set("spark.eventLog.enabled", "true");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqCtx = new SQLContext(jsc);

		JavaRDD<String> stringInput = jsc.textFile("/Users/psingh/Documents/tempFile.csv");
		// create RDD of movie object
		JavaRDD<Movie> dataInput = stringInput.map(new Function<String, Movie>() {

			private static final long serialVersionUID = 1L;

			public Movie call(String str) {
				String[] strArray = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String colorType = strArray[0];
				Integer numFaces = StringUtils.isNotBlank(strArray[15]) ? Integer.parseInt(strArray[15]) : 0;
				Movie dt = new Movie(colorType, numFaces);
				return dt;
			}
		});

		// create dataframe from RDD of movie object
		DataFrame df = sqCtx.createDataFrame(dataInput, com.prakash.spark.models.Movie.class);
		// register dataframe as temp table
		df.registerTempTable("tempTable");
		// write sql to get number of faces total for each color type
		DataFrame sqlRes = sqCtx.sql("select color, sum(numFaces) from tempTable group by color");

		// df.printSchema();
		// display result
		sqlRes.show();

		jsc.close();
	}
}
