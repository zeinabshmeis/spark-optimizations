package aub.edu.lb.spark.optimization.rules;
import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;

public class ReorderRules extends Rule {
	
	private Flow transformation1, transformation2;
	
	public ReorderRules(int id, Flow flow1, Flow flow2, Configuration configuration) {
		super(id, configuration);
		transformation1 = flow1;
		transformation2 = flow2;
	}
	
	public Job getAltJob(Job job) {
		Job newJob = new Job(job.getAction().getClone());
		newJob.getAction().setInput(getAltFlow(job.getAction().getInput()));
		return newJob;
	}
	
	private Flow getAltFlow(Flow flow) {
		if(flow instanceof DataSource) { return flow.getClone(); }
		if(flow == transformation1) {
			// start reorder operation 
			Flow first = transformation2.getClone();
			Flow second = transformation1.getClone();
			if(getId() == ReduceByKeyMapReorder || getId() == FilterSTRedorder) {
				first.setInput(second);
				second.setInput(getAltFlow(transformation2.getInput()));
				return first;
			}
			else if(getId() == FilterJoinReorder1) {
				first.setInput1(second);
				first.setInput2(getAltFlow(transformation2.getInput2()));
				second.setInput(getAltFlow(transformation2.getInput1()));
				return first;
			}
			else if(getId() == FilterJoinReorder2) {
				first.setInput2(second);
				first.setInput1(getAltFlow(transformation2.getInput1()));
				second.setInput(getAltFlow(transformation2.getInput2()));
				return first;
			} 
			else if(getId() == FilterJoinReorder3) {
				first.setInput1(second);
				second.setInput(getAltFlow(transformation2.getInput1()));
				Flow third = transformation1.getClone();
				first.setInput2(third);
				third.setInput(getAltFlow(transformation2.getInput2()));
				return first;
			}
			// end of reorder operation
		}
		Flow newFlow = flow.getClone();
		if(flow instanceof SingleRDDTransformation) {
			newFlow.setInput(getAltFlow(flow.getInput()));
		}
		else if(flow instanceof DoubleRDDTransformation) {
			newFlow.setInput1(getAltFlow(flow.getInput1()));
			newFlow.setInput2(getAltFlow(flow.getInput2()));
		}
		return newFlow;
	}
	
}
