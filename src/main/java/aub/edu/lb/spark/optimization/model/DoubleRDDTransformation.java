package aub.edu.lb.spark.optimization.model;

public abstract class DoubleRDDTransformation implements RDDTransformation{
	
	private Flow input1, input2;
	private boolean visited = false;
	
	protected DoubleRDDTransformation(Flow flow1, Flow flow2) {
		input1 = flow1;
		input2 = flow2;
	}
	
	public Flow getInput1() { return input1; }
	public Flow getInput2() { return input2; }
	public void setInput1(Flow flow) { input1 = flow; }
	public void setInput2(Flow flow) { input2 = flow; }

	public Flow getInput() {return null; }
	public void setInput(Flow flow) { }
	
	public boolean isVisited() { return visited; }
	public void setVisited(boolean visited) { this.visited = visited; }
	
	
	
}
