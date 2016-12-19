package org.linkeddatafragments.solver;


import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;

public abstract class AbstractGraphReorderTransformation implements
        ReorderTransformation {


   protected Graph graph;

   public AbstractGraphReorderTransformation(Graph graph) {
      this.graph = graph;
   }

   @Override
   public final BasicPattern reorder(BasicPattern pattern) {
      return reorderIndexes(pattern).reorder(pattern);
   }

}