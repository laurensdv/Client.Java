package org.linkeddatafragments.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.linkeddatafragments.model.ReifiedTriple;

import java.util.ArrayList;
import java.util.List;

public class OptimizeUtil {

    /**
     * Get a list of variable nodes from a triple
     * @param triple the triple
     * @return a list of the variables.
     */
    public static List<Node> getVariables(Triple triple) {
        List<Node> vars = new ArrayList<>();
        if (triple.getSubject().isVariable()) {
            vars.add(triple.getSubject());
        }
        if (triple.getPredicate().isVariable()) {
            vars.add(triple.getPredicate());
        }
        if (triple.getObject().isVariable()) {
            vars.add(triple.getObject());
        }
        if ((triple instanceof ReifiedTriple) && ((ReifiedTriple)triple).getName().isVariable()){
            vars.add(((ReifiedTriple)triple).getName());
        }
        return vars;
    }
}
