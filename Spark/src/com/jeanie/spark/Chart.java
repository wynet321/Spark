package com.jeanie.spark;

import java.awt.Color;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYSeriesCollection;

public class Chart {
	public static void draw(TimeSeriesCollection timeSeriesCollection) throws IOException {
		String chartFileName = timeSeriesCollection.getSeries(1).getDescription();
		JFreeChart chart = ChartFactory.createXYLineChart(chartFileName.substring(chartFileName.lastIndexOf("/") + 1)
				.replace(".log", ""), "Time Interval", "Seconds", timeSeriesCollection, PlotOrientation.VERTICAL,
				false, true, false);

		XYItemRenderer renderer = new StandardXYItemRenderer();
		renderer.setSeriesPaint(0, Color.BLACK);
		chart.getXYPlot().setRenderer(renderer);
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(chartFileName.replace(".log", ".png")
				.replace("file:", "")));
		ChartUtilities.writeChartAsPNG(out, chart, 350, 263);
		out.close();
	}

	public static void draw(XYSeriesCollection timeSeriesCollection) throws IOException {
		String chartFileName = timeSeriesCollection.getSeries(0).getDescription();
		JFreeChart chart = ChartFactory.createXYLineChart(chartFileName.substring(chartFileName.lastIndexOf("/") + 1)
				.replace(".log", ""), "Time Interval", "Seconds", timeSeriesCollection, PlotOrientation.VERTICAL,
				false, true, false);

		XYItemRenderer renderer = new StandardXYItemRenderer();
		renderer.setSeriesPaint(0, Color.BLACK);
		chart.getXYPlot().setRenderer(renderer);
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(chartFileName.replace(".log", ".png")
				.replace("file:", "")));
		ChartUtilities.writeChartAsPNG(out, chart, 350, 263);
		out.close();
	}
}
