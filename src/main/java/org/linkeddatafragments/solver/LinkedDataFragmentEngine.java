package org.linkeddatafragments.solver;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;

/**
 * Created by ldevocht on 4/28/14.
 */

public class LinkedDataFragmentEngine extends QueryEngineMain {

    protected Query ldfQuery;
    protected DatasetGraph ldfDataset;
    protected Binding ldfBinding;
    protected Context ldfContext;


    public LinkedDataFragmentEngine(Query query, DatasetGraph dataset, Binding input, Context context)
    {
        super(query, dataset, input, context) ;
        this.ldfQuery = query;
        this.ldfDataset = dataset;
        this.ldfBinding = input;
        this.ldfContext = context;
    }

    public LinkedDataFragmentEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    // ---- Registration of the factory for this query engine class.

    // Query engine factory.
    // Call LinkedDataFragmentEngine.register() to add to the global query engine registry.

    private static QueryEngineFactory factory = new LinkedDataFragmentEngineFactory() ;

    static public QueryEngineFactory getFactory() {
        return factory;
    }

    static public void register(){
        QueryEngineRegistry.addFactory(factory) ;
    }

    static public void unregister(){
        QueryEngineRegistry.removeFactory(factory);
    }
}