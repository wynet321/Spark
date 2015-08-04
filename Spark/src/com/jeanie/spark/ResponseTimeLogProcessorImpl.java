package com.jeanie.spark;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import scala.Tuple2;

public class ResponseTimeLogProcessorImpl implements ILogProcessor {
	private static final long serialVersionUID = 1L;

	public void generate(String appName, String url, String filePath) throws Exception {
		if ((appName == null) || (appName.isEmpty())) {
			appName = "dennis";
		}

		if (!url.matches("spark\\:\\/{2}[\\w+\\.]+\\:\\d{1,5}")) {
			throw new Exception("The URL of spark is incorrect. URL=" + url);
		}

		if ((filePath == null) || (filePath.isEmpty())) {
			throw new Exception("The response time log can not be null or empty.");
		}

		System.out.println("Dennis --- Spark work starting.");

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(url);
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaPairRDD<String, String> RDDFileName_Content = context.wholeTextFiles(filePath);

		JavaPairRDD<String, String> RDDFileName_TimeStampResponseTime = RDDFileName_Content
				.filter(new Function<Tuple2<String, String>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, String> s) throws Exception {
						return Boolean.valueOf(!((String) s._2).isEmpty());
					}
				});
		JavaPairRDD<String, Integer> RDDFileNameTimeStamp_ResponseTime = RDDFileName_TimeStampResponseTime
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, Integer>> call(Tuple2<String, String> s) throws Exception {
						String[] newlines = ((String) s._2).split("\n");
						ArrayList<Tuple2<String, Integer>> array = new ArrayList<Tuple2<String, Integer>>();
						for (String unit : newlines) {
							if (unit.isEmpty())
								continue;
							String[] line = unit.split(";");
							if (line.length < 4)
								System.out.println("failed!!!" + unit);
							array.add(new Tuple2<String, Integer>((String) s._1 + ";" + line[0].trim() + " "
									+ line[1].trim(), Integer.valueOf(Integer.parseInt(line[3].trim().substring(0,
									line[3].indexOf(".") - 1)))));
						}
						return array;
					}
				});
		JavaPairRDD<String, Integer> RDDFileNameTimeStamp_ResponsTimeAverage = RDDFileNameTimeStamp_ResponseTime
				.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> s) throws Exception {
						Integer sum = Integer.valueOf(0);
						Integer count = Integer.valueOf(0);
						for (Integer i : s._2) {
							sum = Integer.valueOf(sum.intValue() + i.intValue());
							count = Integer.valueOf(count.intValue() + 1);
						}
						return new Tuple2<String, Integer>((String) s._1,
								new Integer(sum.intValue() / count.intValue()));
					}
				});
		JavaPairRDD<String, String> temp = RDDFileNameTimeStamp_ResponsTimeAverage
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<String, Integer> s) throws Exception {
						String[] fileName_TimeStamp = ((String) s._1).split(";");
						return new Tuple2<String, String>(fileName_TimeStamp[0], fileName_TimeStamp[1] + ";"
								+ ((Integer) s._2).toString());
					}
				});
		JavaRDD<XYSeriesCollection> temp1 = temp.groupByKey().map(
				new Function<Tuple2<String, Iterable<String>>, XYSeriesCollection>() {
					private static final long serialVersionUID = 1L;

					public XYSeriesCollection call(Tuple2<String, Iterable<String>> s) throws Exception {
						XYSeries xySeries = new XYSeries("Response Time");

						xySeries.setDescription(s._1);
						for (String unit : s._2) {
							String[] array = unit.split(";");

							xySeries.add(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(array[0]).getTime(),
									Integer.parseInt(array[1]));
						}

						long firstX = xySeries.getX(0).longValue();
						XYSeries newSeries = new XYSeries("Response Time");
						for (int i = 0; i < xySeries.getItemCount(); i++) {
							newSeries.add(xySeries.getX(i).longValue() - firstX, xySeries.getY(i));
						}

						XYSeriesCollection xySeriesCollection = new XYSeriesCollection();

						xySeriesCollection.addSeries(newSeries);
						return xySeriesCollection;
					}
				});
		temp1.foreach(new VoidFunction<XYSeriesCollection>() {
			private static final long serialVersionUID = 1L;

			public void call(XYSeriesCollection s) throws Exception {
				Chart.draw(s);
			}
		});
		context.close();
		System.out.println("Dennis --- Spark work done.");
	}

	class Sum implements Serializable {
		private static final long serialVersionUID = 1L;
		public int sum = 0;
		public int count = 0;

		Sum() {
		}
	}
}