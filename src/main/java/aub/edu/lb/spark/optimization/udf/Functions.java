package aub.edu.lb.spark.optimization.udf;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import aub.edu.lb.spark.optimization.model.Pair;

/**
 * This is the class contains all the user defined functions used by the transformations
 * @author Zeinab
 *
 */

public final class Functions {
	
	public static HashMap<UnaryOperator, String> unaryFunctionNames = new HashMap<>();
	public static HashMap<BinaryOperator, String> binaryFunctionNames = new HashMap<>();
	
	private static SimpleDateFormat format = new SimpleDateFormat("M/d/y h:m:s a");
	private static Map<String, Double> agencyCX = new HashMap<>();
	private static Map<String, Double> agencyCY = new HashMap<>();
	private static double epsilon;
	
	
	
	
	// -------------------------------------------------------------------- Function For Query 1 --------------------------------------------------------------------
	
}
