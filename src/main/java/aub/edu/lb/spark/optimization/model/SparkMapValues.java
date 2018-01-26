package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.*;

public class SparkMapValues extends SingleRDDTransformation {
	
	private UDF udf;

	public SparkMapValues(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkMapValues(UDF function) {
		super(null);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkMapValues(udf); }

	public String toString() { return "mapValues( " + udf + " ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkMapValues) {
			SparkMapValues mapValues = (SparkMapValues) obj;
			return udf.equals(mapValues.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 191 * udf.hashCode();
	}
}
