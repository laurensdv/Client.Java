package org.linkeddatafragments.utils;

/**
 * Created by ldevocht on 9/2/16.
 */

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;
import org.linkeddatafragments.solver.DefaultCountTransformation;
import org.linkeddatafragments.solver.ReorderQueryIterTriplePattern;
import org.linkeddatafragments.solver.UpdatedStaticCountTransformation;

public class SolverUtil {
    public interface BasicGraphSolverExecutor {
        QueryIterator handle(BasicPattern pattern, QueryIterator input,
                                    ExecutionContext context);
    }

    public static BasicGraphSolverExecutor DEFAULT_SOLVER_EXECUTOR = (pattern, input, context) -> new ReorderQueryIterTriplePattern(pattern, input, context);

    public static BasicPattern optimizeTripleOrder(BasicPattern pattern,
                                                   LinkedDataFragmentGraph graph, ExecutionContext context) {
        // run graph level optimizations
        ReorderTransformation transformation = null;
        if (context.getContext().isTrue(Constants.DYNAMIC_OPTIMIZATION)) {
            transformation = new UpdatedStaticCountTransformation(graph);
        } else if (context.getContext()
                .isTrueOrUndef(Constants.DEFAULT_OPTIMIZATION)) {
            transformation = new DefaultCountTransformation(graph);
        }
        if (null == transformation) {
            return pattern;
        }
        return transformation.reorder(pattern);
    }

    public static QueryIterator solve(BasicPattern pattern, QueryIterator input,
                                      ExecutionContext execCxt, Graph graph, BasicGraphSolverExecutor handler) {

        boolean isLDFGraph = graph instanceof LinkedDataFragmentGraph;

        BasicPattern toProcess = pattern;

        if (isLDFGraph && toProcess.size() > 1) {
            LinkedDataFragmentGraph kg = (LinkedDataFragmentGraph) graph;
            toProcess = optimizeTripleOrder(toProcess, kg, execCxt);
        }

        QueryIterator chain = input;

        chain = processPattern(toProcess, chain, execCxt, handler);

        return chain;
    }

    private static QueryIterator processPattern(BasicPattern pattern,
                                                QueryIterator input, ExecutionContext execCxt,
                                                BasicGraphSolverExecutor handler) {

        QueryIterator result = handler.handle(pattern, input, execCxt);

        return result;
    }

}
