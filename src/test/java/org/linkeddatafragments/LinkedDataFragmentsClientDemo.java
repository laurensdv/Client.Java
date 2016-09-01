package org.linkeddatafragments;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;

/**
 * Created by ldevocht on 9/1/16.
 */
public class LinkedDataFragmentsClientDemo {
    public static void main(String[] args) {
        LinkedDataFragmentGraph ldfg = new LinkedDataFragmentGraph("http://data.linkeddatafragments.org/dbpedia2014");
        Model model = ModelFactory.createModelForGraph(ldfg);

        String queryString = "SELECT ?o ?n WHERE { <http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/almaMater> ?o " +
                ". ?o <http://dbpedia.org/ontology/state> ?n }";
        Query qry = QueryFactory.create(queryString);
        QueryExecution qe = QueryExecutionFactory.create(qry, model);
        ResultSet rs = qe.execSelect();

        while(rs.hasNext()) {
            System.out.println(rs.nextSolution().toString());
        }
    }
}
