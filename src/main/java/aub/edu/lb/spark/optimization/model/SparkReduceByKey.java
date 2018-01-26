package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.*;

public class SparkReduceByKey extends SingleRDDTransformation {
	
	private UDF udf;
	
	public SparkReduceByKey(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkReduceByKey(UDF function) {
		super(null);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkReduceByKey(udf); }

	public String toString() { return "reduceByKey( " + udf + " ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkReduceByKey) {
			SparkReduceByKey reduceByKey = (SparkReduceByKey) obj;
			return udf.equals(reduceByKey.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 197 * udf.hashCode();
	}
}
