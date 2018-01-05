package aub.edu.lb.spark.optimization.model;
public class Job {

	private Action action;
	private int id;
	
	static int countJob = 0;
	
	public Job(Action action) {
		this.action = action;
		id = ++countJob;
	}
	
	public Action getAction() { return action; }
	public void setAction(Action action) { this.action = action; }
	public int getId() { return id; }
	
	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Job) {
			Job job = (Job) obj;
			return id == job.id;
		}
		return false;
	}
	
	public String toString() { return "Job " + id + ":  " + action.toString(); }
	
}
