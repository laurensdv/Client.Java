package org.linkeddatafragments.solver;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ldevocht on 9/2/16.
 */
public class DefaultCountTransformation extends AbstractCountTransformation {

    public DefaultCountTransformation(LinkedDataFragmentGraph graph) {
        super(graph);
    }

    @Override
    public ReorderProc reorderIndexes(BasicPattern pattern) {
        return bgp -> {
            List<Triple> orderedTriples = orderByCounts(bgp.getList(),
                    new ArrayList<>(),
                    1,
                    false);
            BasicPattern result = new BasicPattern();
            orderedTriples.forEach(result::add);
            return result;
        };
    }

}