package aub.edu.lb.spark.optimization.udf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This is the class contains all the user defined functions used by the transformations
 * @author Zeinab
 *
 */

public final class Functions {
	
	public static HashMap<UnaryOperator, String> unaryFunctionNames = new HashMap<>();
	public static HashMap<BinaryOperator, String> binaryFunctionNames = new HashMap<>();
	
	// The Defined Functions
	public static UnaryOperator<String, Iterator<String>> split = (String line) -> { return toIterator(line.split(",")); };
	// End of Defined Functions
	
	static {
		unaryFunctionNames.put(split, "split");
	}
	
	private static <V> Iterator<V> toIterator(V[] array) {
		ArrayList<V> list = new ArrayList<>();
		for(V val : array) list.add(val);
		return list.iterator();
	}
}
