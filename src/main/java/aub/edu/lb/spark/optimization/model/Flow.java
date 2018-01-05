package aub.edu.lb.spark.optimization.model;

public interface Flow {
	
	public Flow getClone();
	public boolean isVisited();
	public void setVisited(boolean visited);
	
	public Flow getInput();
	public Flow getInput1();
	public Flow getInput2();
	
	public void setInput(Flow flow);
	public void setInput1(Flow flow);
	public void setInput2(Flow flow);

}
