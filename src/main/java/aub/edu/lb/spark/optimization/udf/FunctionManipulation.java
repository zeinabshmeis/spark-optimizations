package aub.edu.lb.spark.optimization.udf;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Pair;

public interface FunctionManipulation {
	
	public UDF composePredicates(UDF predicate1, UDF predicate2);
	public UDF composeUDFs(UDF udf1, UDF udf2);
	public UDF changeFunctionDomain(UDF udf);
	public Flow transformUDFToFlow(UDF udf);
	
}
