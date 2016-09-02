package org.linkeddatafragments.solver;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;

/**
 * Created by ldevocht on 9/2/16.
 */
public class LinkedDataFragmentEngineFactory implements QueryEngineFactory {
    @Override
    public boolean accept(Query query, DatasetGraph dataset, Context context) {
        return true;
    }

    @Override
    public Plan create(Query query, DatasetGraph dataset, Binding initial, Context context) {
        LinkedDataFragmentEngine engine = new LinkedDataFragmentEngine(query, dataset, initial, context);
        return engine.getPlan();
    }

    @Override
    public boolean accept(Op op, DatasetGraph dataset, Context context) {
        // Refuse to accept algebra expressions directly.
        return false;
    }

    @Override
    public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
        // Should not be called because accept/Op is false
        throw new ARQInternalErrorException("LDFQueryEngine: factory called directly with an algebra expression");
    }
}
