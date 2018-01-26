package aub.edu.lb.spark.optimization.udf;

public class UDF {

	private String name;
	
	public UDF(String name) { this.name = name; }
	
	public String getName() { return name; }
	
	@Override
	public String toString() { return name; }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof UDF) {
			UDF udf = (UDF) obj;
			return name.equals(udf.getName());
		}
		return false;
	}
	
	@Override
	public int hashCode() { return name.hashCode(); }
	
}
