package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.*;

/**
 * 
 * This class models the reduce action in Spark
 *
 */
public class Reduce extends Action{

	/**
	 * The user defined function provided the reduce function
	 */
	private UDF udf;
	
	/**
	 * 
	 * @param flow
	 * @param function
	 */
	public Reduce(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}

	private Reduce(UDF function) {
		super(null);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	/**
	 * 
	 * @return a new copy of this object but without the input flow
	 */
	public Action getClone() { return new Reduce(udf); }
	
	public String toString() { return "reduce( " + udf + " ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Reduce) {
			Reduce reduce = (Reduce) obj;
			return udf.equals(reduce.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 47 * udf.hashCode();
	}
	
}
