package aub.edu.lb.spark.optimization.model;
public abstract class SingleRDDTransformation implements RDDTransformation{

	private Flow input;
	private boolean visited;
	
	protected SingleRDDTransformation(Flow flow) { input = flow; }
	
	public Flow getInput() { return input; }
	public void setInput(Flow flow) { input = flow; }
	
	public boolean isVisited() { return visited; } 
	public void setVisited(boolean visited) { this.visited = visited; }
	
	public Flow getInput1() { return null; }
	public Flow getInput2() { return null; }

	public void setInput1(Flow flow) {	}
	public void setInput2(Flow flow) {	}
	
	
}
