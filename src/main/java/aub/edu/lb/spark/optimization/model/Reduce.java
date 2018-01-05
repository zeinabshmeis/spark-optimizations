package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.BinaryOperator;
import aub.edu.lb.spark.optimization.udf.Functions;

public class Reduce<V> extends Action{

	private BinaryOperator<V> udf;
	
	public Reduce(Flow flow, BinaryOperator<V> function) {
		super(flow);
		udf = function;
	}

	private Reduce(BinaryOperator<V> function) {
		super(null);
		udf = function;
	}
	
	public BinaryOperator<V> getUDF() { return udf; }
	public void setUDF(BinaryOperator<V> function) { udf = function; }

	public Action getClone() { return new Reduce<V>(udf); }
	
	public String toString() { return "reduce( " + Functions.binaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
	
}
