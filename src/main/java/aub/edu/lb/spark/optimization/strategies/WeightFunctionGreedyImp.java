package aub.edu.lb.spark.optimization.strategies;

import aub.edu.lb.spark.optimization.rules.Rule;

public class WeightFunctionGreedyImp implements WeightFunction{

	@Override
	public int getWeight(int ruleId) {
		if(ruleId == Rule.FilterFilterFusion) return 1;
		else if(ruleId == Rule.MapMapFusion) return 1;
		else if(ruleId == Rule.MapMapValuesFusion) return 1;
		else if(ruleId == Rule.MapValuesMapFusion) return 1;
		else if(ruleId == Rule.FlatMapMapFusion) return 1;
		else if(ruleId == Rule.FlatMapMapValuesFusion) return 1;
		else if(ruleId == Rule.MapValuesMapValuesFusion) return 1;
		else if(ruleId == Rule.FlatMapValuesMapValuesFusion) return 1;
		else if(ruleId == Rule.FilterSTRedorder) return 1;
		else if(ruleId == Rule.ReduceByKeyMapReorder) return 1;
		else if(ruleId == Rule.ReduceTransformationReorder) return 1;
		else if(ruleId == Rule.EliminateEmptyTransformation) return 1;
		else if(ruleId == Rule.FilterJoinReorder1) return 1;
		else if(ruleId == Rule.FilterJoinReorder2) return 1;
		else if(ruleId == Rule.FilterJoinReorder3) return 1;
		else if(ruleId == Rule.FilterSetOpReorder) return 1;
		else if(ruleId == Rule.GroupByAggregate_Map) return 1;
		else if(ruleId == Rule.GroupByAggregate_MapValues) return 1;
		return 1;
	}
	
}
