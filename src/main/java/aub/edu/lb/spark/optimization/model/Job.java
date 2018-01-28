package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class represent a Spark job
 *
 */
public class Job {

	/**
	 * each job has single action that invoked the transformations' evaluation
	 */
	private Action action;
	
	/**
	 * a unique id for that identify each job
	 */
	private int id;
	
	/**
	 * a counter of the number of created jobs
	 */
	static int countJob = 0;

	
	/**
	 * 
	 * @param action an Action object
	 */
	public Job(Action action) {
		this.action = action;
		id = ++countJob;
	}
	
	/**
	 * 
	 * @return the Action of the job
	 */
	public Action getAction() { return action; }
	
	/**
	 * 
	 * @param action an Action object
	 */
	public void setAction(Action action) { this.action = action; }
	
	/**
	 * 
	 * @return the job id
	 */
	public int getId() { return id; }
	
	public String toString() { 
//		return "Job " + id + ":  " + action.toString(); 
		return action.toString();
	}
	
	@Override
	public int hashCode() { 
		return action.toString().hashCode();
//		return action.hashCode() + hashCode(action.getInput()); 
	}
	
	private int hashCode(Flow flow) {
		if(flow instanceof DataSource) return flow.hashCode();
		if(flow instanceof SingleRDDTransformation) return flow.hashCode() + hashCode(flow.getInput());
		return flow.hashCode() + hashCode(flow.getInput1()) + hashCode(flow.getInput2());
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Job) {
			Job job = (Job) obj;
//			if(!action.equals(job.getAction())) return false;
			return action.toString().equals(job.getAction().toString());
//			return compareFlows(action.getInput(), job.getAction().getInput());
		}
		return false;
	}
	
	private boolean compareFlows(Flow flow1, Flow flow2) {
		if(!flow1.equals(flow2)) return false;
		if(flow1 instanceof DataSource) return true;
		if(flow1 instanceof SingleRDDTransformation) return compareFlows(flow1.getInput(), flow2.getInput());
		return compareFlows(flow1.getInput1(), flow2.getInput1()) && compareFlows(flow1.getInput2(), flow2.getInput2());
	}
}
