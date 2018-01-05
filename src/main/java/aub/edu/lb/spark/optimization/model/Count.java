package aub.edu.lb.spark.optimization.model;

public class Count extends Action {

	public Count(Flow flow) {
		super(flow);
	}

	private Count() {
		super(null);
	}

	@Override
	public Action getClone() {
		return new Count();
	}
	
	public String toString() { return "count() º " + getInput().toString();	}
}
