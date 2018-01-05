package aub.edu.lb.spark.optimization;

import aub.edu.lb.spark.optimization.checker.*;
import aub.edu.lb.spark.optimization.model.*;
import aub.edu.lb.spark.optimization.optimizer.Optimizer;
import aub.edu.lb.spark.optimization.udf.*;

/**
 * 
 * @author zeinabshmeiss
 *
 */
public class App {
	public static void main(String[] args) {
		
		// create the configuration
		PropertyChecker propertyChecker = new PropertyCheckerDefaultImp();
		FunctionManipulation functionManipulation = new FunctionManipulationDefaultImp();
		Configuration configuration = new Configuration(propertyChecker, functionManipulation);
		
		// create the job
		DataSource source = new TextFile("");
		SparkMap map1 = new SparkMap<>(source, null);
		SparkFlatMap map2 = new SparkFlatMap<>(map1, null);
		SparkMap map3 = new SparkMap<>(map2, null);
		Action count = new Count(map3);
		Job job = new Job(count);
		
		// create the optimizer
		Optimizer optimizer = new Optimizer(job, configuration);
		optimizer.synthesis();
		
		System.out.println("The following alternative jobs: ");
		for (Job alt : optimizer.getAlternativeJobs()) { System.out.println(alt.toString()); }
	}
	
}
