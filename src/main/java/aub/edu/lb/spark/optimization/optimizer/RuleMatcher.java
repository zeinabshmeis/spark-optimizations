package aub.edu.lb.spark.optimization.optimizer;

import java.util.ArrayList;

import aub.edu.lb.spark.optimization.checker.Configuration;
import aub.edu.lb.spark.optimization.model.SparkFilter;
import aub.edu.lb.spark.optimization.model.SparkFlatMap;
import aub.edu.lb.spark.optimization.model.SparkFlatMapValues;
import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.model.SparkGroupByKey;
import aub.edu.lb.spark.optimization.model.SparkJoin;
import aub.edu.lb.spark.optimization.model.SparkMap;
import aub.edu.lb.spark.optimization.model.SparkMapValues;
import aub.edu.lb.spark.optimization.model.SparkReduceByKey;
import aub.edu.lb.spark.optimization.model.SingleRDDTransformation;
import aub.edu.lb.spark.optimization.rules.EliminationRule;
import aub.edu.lb.spark.optimization.rules.ExplorationRule;
import aub.edu.lb.spark.optimization.rules.FusionRule;
import aub.edu.lb.spark.optimization.rules.GroupByAggregateRule;
import aub.edu.lb.spark.optimization.rules.ReorderRules;
import aub.edu.lb.spark.optimization.rules.Rule;

/**
 * 
 * This class represents the module that checks for the rules that can applied 
 * for optimizing a Spark job starting from a specific point
 *
 */
public class RuleMatcher {
	
	public static ArrayList<Rule> getMatches(Flow flow, Configuration configuration) {
		ArrayList<Rule> rules = new ArrayList<>();
		
		if(flow instanceof SparkMap && configuration.getPropertyChecker().isIdentityOperation(((SparkMap)flow).getUDF())) {
			rules.add(new EliminationRule(Rule.EliminateEmptyTransformation, flow, configuration));
		}
		if(flow instanceof SparkMap && flow.getInput() instanceof SparkMap) {
			rules.add(new FusionRule(Rule.MapMapFusion, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkFlatMap && flow.getInput() instanceof SparkMap) {
			rules.add(new FusionRule(Rule.FlatMapMapFusion, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkMapValues && flow.getInput() instanceof SparkMapValues) {
			rules.add(new FusionRule(Rule.MapValuesMapValuesFusion, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkFlatMapValues && flow.getInput() instanceof SparkMapValues) {
			rules.add(new FusionRule(Rule.FlatMapValuesMapValuesFusion, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkFilter && flow.getInput() instanceof SparkFilter) {
			rules.add(new FusionRule(Rule.FilterFilterFusion, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkReduceByKey && flow.getInput() instanceof SparkMap
				&& configuration.getPropertyChecker().isDistributive(((SparkMap)flow.getInput()).getUDF(), ((SparkReduceByKey)flow).getUDF())) {
			rules.add(new ReorderRules(Rule.ReduceByKeyMapReorder, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkFilter && flow.getInput() instanceof SingleRDDTransformation && !(flow.getInput() instanceof SparkFilter) 
				&& !configuration.getPropertyChecker().hasReadWriteConflict(flow, flow.getInput())) {
			rules.add(new ReorderRules(Rule.FilterSTRedorder, flow, flow.getInput(), configuration));
		}
		if(flow instanceof SparkFilter && flow.getInput() instanceof SparkJoin) {
			if(configuration.getPropertyChecker().readSetIsSubsetOfVarSet(flow, flow.getInput().getInput1())) {
				rules.add(new ReorderRules(Rule.FilterJoinReorder1, flow, flow.getInput(), configuration));
			}
			if(configuration.getPropertyChecker().readSetIsSubsetOfVarSet(flow, flow.getInput().getInput2())) {
				rules.add(new ReorderRules(Rule.FilterJoinReorder2, flow, flow.getInput(), configuration));
			}
		}
		if(flow instanceof SparkMap && flow.getInput() instanceof SparkGroupByKey) {
			rules.add(new GroupByAggregateRule(Rule.GroupByAggregate, flow, configuration));
		}
		if(flow instanceof SparkMap && flow.getInput() instanceof SparkMapValues) {
			rules.add(new ExplorationRule(Rule.MapMapValuesSubstitution, flow.getInput(), configuration));
		}
		if(flow instanceof SparkMapValues && flow.getInput() instanceof SparkMap) {
			rules.add(new ExplorationRule(Rule.MapValuesMapSubstitution, flow, configuration));
		}
		if(flow instanceof SparkFlatMap && flow.getInput() instanceof SparkMapValues) {
			rules.add(new ExplorationRule(Rule.MapMapValuesSubstitution, flow.getInput(), configuration));
		}
		if(flow instanceof SparkMapValues && flow.getInput() instanceof SparkGroupByKey) {
			rules.add(new ExplorationRule(Rule.MapValuesGroupByKeySubstitution, flow, configuration));
		}
		return rules;
	}
}
