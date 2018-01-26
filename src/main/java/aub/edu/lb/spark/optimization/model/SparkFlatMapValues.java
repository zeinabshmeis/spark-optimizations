package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.*;


public class SparkFlatMapValues extends SingleRDDTransformation {
	
	private UDF udf;

	public SparkFlatMapValues(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkFlatMapValues(UDF function) {
		super(null);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkFlatMapValues(udf); }
	
	public String toString() { return "flatMapValues( " + udf + " ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkFlatMapValues) {
			SparkFlatMap flatMapValues = (SparkFlatMap) obj;
			return udf.equals(flatMapValues.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 97 * udf.hashCode();
	}
}
