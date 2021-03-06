package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class models the collect action in Spark
 *
 */
public class Collect extends Action{

	/**
	 * 
	 * @param flow a Flow object that represents the computation that
	 *             creates the RDD on which the action is invoked
	 */
	public Collect(Flow flow) { super(flow); }
	
	private Collect() { super(null); }

	/**
	 * @return returns a new instance of this object but without the input flow
	 */
	public Action getClone() { return new Collect(); }

	@Override
	public String toString() { return "collect() • " + getInput().toString();	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Collect) return true;
		return false;
	}
	
	@Override
	public int hashCode() {
		return 17;
	}

}
