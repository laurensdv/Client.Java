package org.linkeddatafragments.solver;

import org.linkeddatafragments.model.LinkedDataFragmentGraph;

/**
 * Created by ldevocht on 9/2/16.
 */

public abstract class AbstractLDFGraphReorderTransformation extends AbstractGraphReorderTransformation {

    public AbstractLDFGraphReorderTransformation(LinkedDataFragmentGraph graph) {
        super(graph);
    }

    protected LinkedDataFragmentGraph getGraph() {
        return (LinkedDataFragmentGraph)graph;
    }

}