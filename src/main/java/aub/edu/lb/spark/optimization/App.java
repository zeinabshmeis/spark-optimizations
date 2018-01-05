package aub.edu.lb.spark.optimization;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.checker.PropertyChecker;
import aub.edu.lb.spark.optimization.model.Action;
import aub.edu.lb.spark.optimization.model.Count;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.SparkFlatMap;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SparkMap;
import aub.edu.lb.spark.optimization.model.Pair;
import aub.edu.lb.spark.optimization.model.TextFile;
import aub.edu.lb.spark.optimization.optimizer.JobOptimizer;
import aub.edu.lb.spark.optimization.optimizer.SearchSpace;
import aub.edu.lb.spark.optimization.udf.BinaryOperator;
import aub.edu.lb.spark.optimization.udf.FunctionManipulation;
import aub.edu.lb.spark.optimization.udf.Predicate;
import aub.edu.lb.spark.optimization.udf.UnaryOperator;

/**
 * 
 * @author zeinabshmeiss
 *
 */
public class App {
	public static void main(String[] args) {
		
		// create the configuration
		PropertyChecker propertyChecker = new PropertyCheckerImp();
		FunctionManipulation functionManipulation = new FunctionManipulationImp();
		Configuration configuration = new Configuration(propertyChecker, functionManipulation);
		
		// create the job
		DataSource source = new TextFile("");
		SparkMap map1 = new SparkMap<>(source, null);
		SparkFlatMap map2 = new SparkFlatMap<>(map1, null);
		SparkMap map3 = new SparkMap<>(map2, null);
		Action count = new Count(map3);
		Job job = new Job(count);
		
		// create the search space
		SearchSpace searchSpace = new SearchSpace();
		searchSpace.addJob(job);
		
		JobOptimizer op = new JobOptimizer(job, searchSpace, configuration);
		op.optimize();

		System.out.println("The following alternative jobs: ");
		for (Job alt : searchSpace.getJobs()) { System.out.println(alt.toString()); }
	}
	
	
	private static class PropertyCheckerImp implements PropertyChecker {

		@Override
		public <V> boolean isIdentityOperation(UnaryOperator<V, V> operation) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public <V> boolean isDistributive(UnaryOperator<V, V> operation1,
				BinaryOperator<V> operation2) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean hasReadWriteConflict(Flow flow1, Flow flow2) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean readSetIsSubsetOfVarSet(Flow flow1, Flow flow2) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
	private static class FunctionManipulationImp implements FunctionManipulation {

		@Override
		public <V> Predicate<V> composePredicates(Predicate<V> predicate1,
				Predicate<V> predicate2) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <V, T, U> UnaryOperator<V, U> composeUDFs(
				UnaryOperator<V, T> udf1, UnaryOperator<T, U> udf2) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <V, U, K> UnaryOperator<V, U> changeFunctionDomain(
				UnaryOperator<V, U> udf) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <V, U, K> Flow transformUDFToFlow(
				UnaryOperator<Pair<K, V>, Pair<K, U>> udf) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
