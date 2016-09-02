package org.linkeddatafragments.model;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.linkeddatafragments.utils.OptimizeUtil;

import java.util.ArrayList;
import java.util.List;

public class TriplePatternCount {
	public Triple triple;
	public long count;
	public long estimate;
	public List<Node> unboundVariables = new ArrayList<>();

	public TriplePatternCount(Triple triple, long count) {
		this.triple = triple;
		this.count = count;
		estimate = 0;
		unboundVariables = OptimizeUtil.getVariables(triple);
	}

	@Override
	public String toString() {
		return triple.toString() + "Count: " + count + " Estimate: " + estimate;
	}
}
