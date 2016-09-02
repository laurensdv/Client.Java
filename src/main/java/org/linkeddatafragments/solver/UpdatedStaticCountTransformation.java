package org.linkeddatafragments.solver;


import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;

import java.util.ArrayList;
import java.util.List;

public class UpdatedStaticCountTransformation extends AbstractCountTransformation {

   public UpdatedStaticCountTransformation(LinkedDataFragmentGraph graph) {
      super(graph);
   }

   @Override
   public ReorderProc reorderIndexes(BasicPattern pattern) {
      return bgp -> {
         List<Triple> orderedTriples = orderByCounts(bgp.getList(),
                                                     new ArrayList<>(),
                                                     1,
                                                     true);
         BasicPattern result = new BasicPattern();
         orderedTriples.forEach(result::add);
         return result;
      };
   }

}
