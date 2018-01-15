package aub.edu.lb.spark.optimization.udf;


import aub.edu.lb.spark.optimization.model.*;

public class FunctionManipulationDefaultImp implements FunctionManipulation{

	@Override
	public UDF composePredicates(UDF predicate1, UDF predicate2) {
		return new UDF(predicate1 + "--" + predicate2);
	}

	@Override
	public UDF composeUDFs(UDF udf1, UDF udf2) {
		return new UDF(udf1 + "--" + udf2);
	}

	@Override
	public UDF changeFunctionDomain(UDF udf) {
		return new UDF("(" + udf + "--WithKey)");
	}

	@Override
	public Flow transformUDFToFlow(UDF udf) {
		if(udf.toString().equals("(size--WithKey)")) {
			SparkMap map = new SparkMap(null, new UDF("(Agency, 1)"));
			SparkReduceByKey reduceByKey = new SparkReduceByKey(map, new UDF("sum"));
			return reduceByKey;
		}
		else if(udf.toString().equals("(variance--WithKey)")) {
			SparkMap map1 = new SparkMap(null, new UDF("(KMinusAvg--WithKey)"));
			SparkMap map2 = new SparkMap(map1, new UDF("(squareQ2--WithKey)"));
			SparkMap map3 = new SparkMap(map2, new UDF("(devideNminusOne--WithKey)"));
			SparkReduceByKey reduceByKey = new SparkReduceByKey(map3, new UDF("sum"));
			return reduceByKey;
		}
		return null;
	}
}
