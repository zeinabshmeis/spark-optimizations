package aub.edu.lb.spark.optimization.rules;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.DataSource;
import aub.edu.lb.spark.optimization.model.DoubleRDDTransformation;
import aub.edu.lb.spark.optimization.model.SparkFilter;
import aub.edu.lb.spark.optimization.model.SparkFlatMap;
import aub.edu.lb.spark.optimization.model.SparkFlatMapValues;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.Job;
import aub.edu.lb.spark.optimization.model.SparkMap;
import aub.edu.lb.spark.optimization.model.SparkMapValues;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;
import aub.edu.lb.spark.optimization.udf.FunctionManipulation;

public class FusionRule extends Rule{
	
	private Flow transformation1, transformation2;
	
	public FusionRule(int id, Flow flow1, Flow flow2, Configuration configuration) {
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
			Flow newFlow = compose();
			newFlow.setInput(getAltFlow(newFlow.getInput()));
			return newFlow;
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
	
	private Flow compose() {
		FunctionManipulation funcMan = getConfiguration().getFunctionsManipulation();
		if(getId() == MapMapFusion) {
			return new SparkMap(transformation2.getInput(), funcMan.composeUDFs(((SparkMap) transformation2).getUDF(), ((SparkMap) transformation1).getUDF()));
		}
		else if(getId() == MapValuesMapValuesFusion) {
			return new SparkMapValues(transformation2.getInput(), funcMan.composeUDFs(((SparkMapValues) transformation2).getUDF(), ((SparkMapValues) transformation1).getUDF()));
		}
		else if(getId() == MapMapValuesFusion) {
			return new SparkMap(transformation2.getInput(), funcMan.composeUDFs(funcMan.changeFunctionDomain(((SparkMapValues) transformation2).getUDF()), ((SparkMap)transformation1).getUDF()));
		}
		else if(getId() == MapValuesMapFusion) {
			return new SparkMap(transformation2.getInput(), funcMan.composeUDFs(((SparkMap)transformation2).getUDF(), funcMan.changeFunctionDomain(((SparkMapValues)transformation1).getUDF())));
		}
		else if(getId() == FlatMapMapFusion) {
			return new SparkFlatMap(transformation2.getInput(), funcMan.composeUDFs(((SparkMap) transformation2).getUDF(), ((SparkFlatMap) transformation1).getUDF()));
		}
		else if(getId() == FlatMapMapValuesFusion) {
			return new SparkFlatMap(transformation2.getInput(), funcMan.composeUDFs(funcMan.changeFunctionDomain(((SparkMapValues)transformation2).getUDF()), ((SparkFlatMap)transformation1).getUDF()));
		}
		else if(getId() == FlatMapValuesMapValuesFusion) {
			return new SparkFlatMapValues(transformation2.getInput(), funcMan.composeUDFs(((SparkMapValues) transformation2).getUDF(), ((SparkFlatMapValues) transformation1).getUDF()));
		}
		else if(getId() == FilterFilterFusion) {
			return new SparkFilter(transformation2.getInput(), funcMan.composePredicates(((SparkFilter) transformation2).getUDF(), ((SparkFilter) transformation1).getUDF()));
		}
		return null;
	}

}