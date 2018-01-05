package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.Functions;
import aub.edu.lb.spark.optimization.udf.UnaryOperator;

public class SparkMap<V, U> extends SingleRDDTransformation {
	
	private UnaryOperator<V, U> udf;

	public SparkMap(Flow flow, UnaryOperator<V, U> function) {
		super(flow);
		udf = function;
	}
	
	private SparkMap(UnaryOperator<V, U> function, boolean visited) {
		super(null);
		setVisited(visited);
		udf = function;
	}
	
	public UnaryOperator<V, U> getUDF() { return udf; }
	public void setUDF(UnaryOperator<V, U> function) { udf = function; }

	public Flow getClone() { return new SparkMap<V, U>(udf, super.isVisited()); }
	
	public String toString() { return "map( " + Functions.unaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
	
}
