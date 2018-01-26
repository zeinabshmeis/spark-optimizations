package aub.edu.lb.spark.optimization.optimizer;

import java.util.ArrayList;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.checker.PropertyChecker;
import aub.edu.lb.spark.optimization.model.*;
import aub.edu.lb.spark.optimization.rules.*;

/**
 * 
 * This class represents the module that checks for the rules that can applied 
 * for optimizing a Spark job starting from a specific point
 *
 */
public class RuleMatcher {

	public static ArrayList<Rule> getMatches(Job job, Configuration configuration) {
		ArrayList<Rule> rules = new ArrayList<>();
		for(int ruleId = 1; ruleId <= Rule.NUMBER_OF_RULES; ruleId++) {
			Rule rule = ruleMatch(job, configuration, ruleId);
			if(rule != null) rules.add(rule);
		}
		return rules;
	}
	
	// this method returns the first Redex of rule ruleId in job
	public static Rule ruleMatch(Job job, Configuration configuration, int ruleId) {
		return ruleMatch(job.getAction().getInput(), configuration, ruleId);
	}
	
	private static Rule ruleMatch(Flow flow, Configuration configuration, int ruleId) {
		if(flow instanceof DataSource) return null;
		
		PropertyChecker propCheck = configuration.getPropertyChecker();
		
		if(ruleId == Rule.FilterFilterFusion) {
			if(flow instanceof SparkFilter && flow.getInput() instanceof SparkFilter) 
				new FusionRule(Rule.FilterFilterFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.MapMapFusion) {
			if(flow instanceof SparkMap && flow.getInput() instanceof SparkMap)
				return new FusionRule(Rule.MapMapFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.MapMapValuesFusion) {
			if(flow instanceof SparkMap && flow.getInput() instanceof SparkMapValues) 
				return new FusionRule(Rule.MapMapValuesFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.MapValuesMapFusion) {
			if(flow instanceof SparkMapValues && flow.getInput() instanceof SparkMap) 
				return new FusionRule(Rule.MapValuesMapFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FlatMapMapFusion) {
			if(flow instanceof SparkFlatMap && flow.getInput() instanceof SparkMap)
				return new FusionRule(Rule.FlatMapMapFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FlatMapMapValuesFusion) {
			if(flow instanceof SparkFlatMap && flow.getInput() instanceof SparkMapValues)
				return new FusionRule(Rule.FlatMapMapValuesFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.MapValuesMapValuesFusion) {
			if(flow instanceof SparkMapValues && flow.getInput() instanceof SparkMapValues)
				return new FusionRule(Rule.MapValuesMapValuesFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FlatMapValuesMapValuesFusion) {
			if(flow instanceof SparkFlatMapValues && flow.getInput() instanceof SparkMapValues) 
				return new FusionRule(Rule.FlatMapValuesMapValuesFusion, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FilterSTRedorder) {
			if(flow instanceof SparkFilter && flow.getInput() instanceof SingleRDDTransformation && !propCheck.hasReadWriteConflict(flow, flow.getInput())) 
				return new ReorderRules(Rule.FilterSTRedorder, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.ReduceByKeyMapReorder) {
			if(flow instanceof SparkReduceByKey && flow.getInput() instanceof SparkMap && propCheck.isDistributive(((SparkMap)flow.getInput()).getUDF(), ((SparkReduceByKey)flow).getUDF())) 
				return new ReorderRules(Rule.ReduceByKeyMapReorder, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.ReduceTransformationReorder) {
			return null;
		}
		else if(ruleId == Rule.EliminateEmptyTransformation) {
			if(flow instanceof SparkMap && propCheck.isIdentityOperation(((SparkMap)flow).getUDF()))
				return new EliminationRule(Rule.EliminateEmptyTransformation, flow, configuration);
		}
		else if(ruleId == Rule.FilterJoinReorder1) {
			if(flow instanceof SparkFilter && flow.getInput() instanceof SparkJoin && propCheck.readSetIsSubsetOfVarSet(flow, flow.getInput().getInput1()))
				return new ReorderRules(Rule.FilterJoinReorder1, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FilterJoinReorder2) {
			if(flow instanceof SparkFilter && flow.getInput() instanceof SparkJoin && propCheck.readSetIsSubsetOfVarSet(flow, flow.getInput().getInput2()))
				return new ReorderRules(Rule.FilterJoinReorder2, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FilterJoinReorder3) {
			if(flow instanceof SparkFilter && flow.getInput() instanceof SparkJoin && propCheck.readSetIsSubsetOfVarSet(flow, flow.getInput().getInput1()) && propCheck.readSetIsSubsetOfVarSet(flow, flow.getInput().getInput2()))
				return new ReorderRules(Rule.FilterJoinReorder3, flow, flow.getInput(), configuration);
		}
		else if(ruleId == Rule.FilterSetOpReorder) {
			return null;
		}
		else if(ruleId == Rule.GroupByAggregate_Map) {
			if(flow instanceof SparkMap && flow.getInput() instanceof SparkGroupByKey) 
				return new GroupByAggregateRule(Rule.GroupByAggregate_Map, flow, configuration);
		}
		else if(ruleId == Rule.GroupByAggregate_MapValues) {
			if(flow instanceof SparkMapValues && flow.getInput() instanceof SparkGroupByKey) 
				return new GroupByAggregateRule(Rule.GroupByAggregate_MapValues, flow, configuration);
		}
		
		// rule does not match the current operator 
		if(flow instanceof SingleRDDTransformation) {
			return ruleMatch(flow.getInput(), configuration, ruleId);
		}
		Rule rule = ruleMatch(flow.getInput1(), configuration, ruleId);
		if(rule != null) return rule;
		return ruleMatch(flow.getInput2(), configuration, ruleId);
	}
	
}
