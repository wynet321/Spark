package com.jeanie.spark;

public class Launcher {
	public static void main(String[] args) throws Exception {
		ILogProcessor log = new ResponseTimeLogProcessorImpl();
		log.generate("dennis", "spark://Spark1.cn.ibm.com:7077", "/log");
	}
}