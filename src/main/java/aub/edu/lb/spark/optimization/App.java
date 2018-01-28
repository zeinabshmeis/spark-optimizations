package aub.edu.lb.spark.optimization;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import aub.edu.lb.spark.optimization.checker.*;
import aub.edu.lb.spark.optimization.model.*;
import aub.edu.lb.spark.optimization.optimizer.Optimizer;
import aub.edu.lb.spark.optimization.strategies.*;
import aub.edu.lb.spark.optimization.udf.*;

/**
 * 
 * @author zeinabshmeiss
 *
 */
public class App {
	
	public static void main(String[] args) throws IOException {
		
		// create the configuration
		PropertyChecker propertyChecker = new PropertyCheckerDefaultImp();
		FunctionManipulation functionManipulation = new FunctionManipulationDefaultImp();
		Configuration configuration = new Configuration(propertyChecker, functionManipulation);
		
		// create the job
		Job job = generateQ1();
		
		// create the optimizer
		Optimizer optimizer = new Optimizer(job, configuration);
		optimizer.synthesis();
		
		PrintWriter out = new PrintWriter(new FileWriter("search-space.txt"));
		out.println("The following alternative jobs: ");
		for (Job alt : optimizer.getAlternativeJobs()) { out.println(alt.toString()); }
		out.close();
		
		// select the most optimal job based on strategies
		System.out.println("Max Wieght Greedy");
		System.out.println(optimizer.select(new MaxWeightStrategy(new WeightFunctionGreedyImp(), false)));
		System.out.println("\n Max Wieght");
		System.out.println(optimizer.select(new MaxWeightStrategy(new WeightFunctionImp(), false)));
		System.out.println("\n Max Wieght Prunning");
		System.out.println(optimizer.select(new MaxWeightStrategy(new WeightFunctionImp(), true)));
		System.out.println("\n Min Cost Greedy");
		System.out.println(optimizer.select(new MinCostStrategy(new CostFunctionGreedyImp(), false)));
		System.out.println("\n Min Cost");
		System.out.println(optimizer.select(new MinCostStrategy(new CostFunctionImp(), false)));
		System.out.println("\n Min Wieght Pruning");
		System.out.println(optimizer.select(new MinCostStrategy(new CostFunctionImp(), true)));
		
	}
	
	
	
	/**
	 * 
	 * @return job that computes the number of calls for each Agency during busy hours and within a specific distance from the Agency itself
	 */
	public static Job generateQ1() {
		// generate the flow that computes the number of calls for each Hour
		DataSource source1 = new TextFile("data.txt");
		SparkMap map1 = new SparkMap(source1, new UDF("split"));
		SparkMap map2 = new SparkMap(map1, new UDF("extractHour"));
		SparkReduceByKey reduceByKey1 = new SparkReduceByKey(map2, new UDF("sum"));
		
		// generate the flow that computes the for each call the Hour, Agency, and the distance between the call and the agency 
		DataSource source2 = new TextFile("data.txt");
		SparkMap map3 = new SparkMap(source2, new UDF("split"));
		SparkMap map4 = new SparkMap(map3, new UDF("extractHourAgencyLocation"));
		SparkMapValues mapValues1 = new SparkMapValues(map4, new UDF("subtractAgency'sCenter"));
		SparkMapValues mapValues2 = new SparkMapValues(mapValues1, new UDF("locationSquare"));
		SparkMapValues mapValues3 = new SparkMapValues(mapValues2, new UDF("sumXY"));
		SparkMapValues mapValues4 = new SparkMapValues(mapValues3, new UDF("sqrt"));
		
		// join and filter then get size
		SparkJoin join = new SparkJoin(reduceByKey1, mapValues4);
		SparkFilter filter1 = new SparkFilter(join, new UDF("filterDistance"));
		SparkFilter filter2 = new SparkFilter(filter1, new UDF("filterHourCount"));
		SparkFilter filter3 = new SparkFilter(filter2, new UDF("filterMidNight"));
		SparkMap map5 = new SparkMap(filter3, new UDF("leaveAgencyDistance"));
		SparkGroupByKey groupByKey = new SparkGroupByKey(map5);
		SparkMapValues mapValues5 = new SparkMapValues(groupByKey, new UDF("size"));
		Collect collect = new Collect(mapValues5);
		return new Job(collect);
	}
	
	
	/**
	 * 
	 * @return job that computes for each Agency the variance of the set of calls made by from each city
	 */
	public static Job generateQ2() {
		// generate for each agency the average number of calls made by the cities
		DataSource source1 = new TextFile("data.txt");
		SparkMap map1 = new SparkMap(source1, new UDF("split"));
		SparkMap map2 = new SparkMap(map1, new UDF("extractAgencyQ2"));
		SparkReduceByKey reduceByKey1 = new SparkReduceByKey(map2, new UDF("sum"));
		SparkMap map3 = new SparkMap(reduceByKey1, new UDF("devideByNCities")); 
		
		// get the number of calls made from each city for each agency
		DataSource source2 = new TextFile("data.txt");
		SparkMap map4 = new SparkMap(source2, new UDF("split"));
		SparkMap map5 = new SparkMap(map4, new UDF("extractHourAgencyCityK"));
		
		// join and get variance
		SparkJoin join = new SparkJoin(map3, map5);
		SparkGroupByKey groupByKey = new SparkGroupByKey(join);
		SparkMapValues mapValues = new SparkMapValues(groupByKey, new UDF("variance"));
		Collect collect = new Collect(mapValues);
		return new Job(collect);
	}
		
	
	/**
	 * 
	 * @return job that computes for each Agency the variance of the set of calls made by from each city
	 */
	public static Job generateQ3() {
		// generate the flow that computes the for each call the Hour, Agency, and the distance between the call and the agency 
		DataSource source = new TextFile("data.txt");
		SparkMap map1 = new SparkMap(source, new UDF("split"));
		SparkMap map2 = new SparkMap(map1, new UDF("extractHourAgencyLocation"));
		SparkMapValues mapValues1 = new SparkMapValues(map2, new UDF("subtractAgency'sCenter"));
		SparkMapValues mapValues2 = new SparkMapValues(mapValues1, new UDF("locationSquare"));
		SparkMapValues mapValues3 = new SparkMapValues(mapValues2, new UDF("sumXY"));
		SparkMapValues mapValues4 = new SparkMapValues(mapValues3, new UDF("sqrt"));
		SparkFilter filter1 = new SparkFilter(mapValues4, new UDF("filterDistance"));
		SparkFilter filter2 = new SparkFilter(filter1, new UDF("filterHourCount"));
		SparkMap map5 = new SparkMap(filter2, new UDF("leaveAgencyDistance"));
		SparkGroupByKey groupByKey = new SparkGroupByKey(map5);
		SparkMapValues mapValues5 = new SparkMapValues(groupByKey, new UDF("size"));
		Collect collect = new Collect(mapValues5);
		return new Job(collect);
	}
}
