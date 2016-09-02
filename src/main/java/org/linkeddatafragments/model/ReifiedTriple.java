package org.linkeddatafragments.model;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.shared.PrefixMapping;

/**
 * Created by ldevocht on 9/2/16.
 */
public class ReifiedTriple extends Triple {

    private Node name;

    public ReifiedTriple(Node name, Node s, Node p, Node o){
        super(s, p, o);
        this.name = name;
    }


    public Node getMatchName(){
        return Node.ANY.equals( name ) ? null : name;
    }

    public Node getName(){
        return name;
    }




    @Override
    public String toString(PrefixMapping pm) {
        return "["+name.toString(pm)+"] "+super.toString(pm);
    }


}