package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class represent a Spark job
 *
 */
public class Job implements Comparable<Job>{

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
	 */
	private int cost;
	
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
	
	public void setCost(int cost) { this.cost = cost; }
	
	public int getCost() { return cost; }
	
	@Override
	public int hashCode() { return id; }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Job) {
			Job job = (Job) obj;
			return id == job.id;
		}
		return false;
	}
	
	public String toString() { return "Job " + id + ":  " + action.toString(); }

	@Override
	public int compareTo(Job o) {
		if(cost < o.cost) return -1;
		if(cost > o.cost) return 1;
		return 0;
	}
	
}
