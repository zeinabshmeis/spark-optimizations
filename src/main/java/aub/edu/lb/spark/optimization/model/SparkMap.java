package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.*;

public class SparkMap extends SingleRDDTransformation {
	
	private UDF udf;

	public SparkMap(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkMap(UDF function) {
		super(null);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }

	public Flow getClone() { return new SparkMap(udf); }
	
	public String toString() { return "map( " + udf + " ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkMap) {
			SparkMap map = (SparkMap) obj;
			return udf.equals(map.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 179 * udf.hashCode();
	}
	
}
