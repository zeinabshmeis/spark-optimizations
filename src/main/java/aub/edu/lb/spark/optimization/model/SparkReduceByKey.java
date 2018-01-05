package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.BinaryOperator;
import aub.edu.lb.spark.optimization.udf.Functions;

public class SparkReduceByKey<K, V> extends SingleRDDTransformation {
	
	private BinaryOperator<V> udf;
	
	public SparkReduceByKey(Flow flow, BinaryOperator<V> function) {
		super(flow);
		udf = function;
	}
	
	private SparkReduceByKey(BinaryOperator<V> function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public BinaryOperator<V> getUDF() { return udf; }
	public void setUDF(BinaryOperator<V> function) { udf = function; }

	public Flow getClone() { return new SparkReduceByKey<K, V>(udf, super.isVisited()); }

	public String toString() { return "reduceByKey( " + Functions.binaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
}
