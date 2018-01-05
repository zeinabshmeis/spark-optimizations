package aub.edu.lb.spark.optimization.model;
public class Collect extends Action{

	public Collect(Flow flow) { super(flow); }
	
	private Collect() {
		super(null);
	}

	public Action getClone() { return new Collect(); }
	
	public String toString() { return "collect() º " + getInput().toString();	}

}
