package org.linkeddatafragments;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;

/**
 * Created by ldevocht on 9/1/16.
 */
public class LinkedDataFragmentsClientDemo {
    public static void main(String[] args) {
        LinkedDataFragmentGraph ldfg = new LinkedDataFragmentGraph("http://data.linkeddatafragments.org/dbpedia2014");
        Model model = ModelFactory.createModelForGraph(ldfg);

        String queryString = "SELECT DISTINCT ?n WHERE { <http://dbpedia.org/resource/Barack_Obama> ?p ?o " +
                ". ?o ?p1 ?o1 . ?o1 <http://dbpedia.org/ontology/state> ?n } LIMIT 4";
        Query qry = QueryFactory.create(queryString);
        QueryExecution qe = QueryExecutionFactory.create(qry, model);
        ResultSet rs = qe.execSelect();

        while(rs.hasNext()) {
            System.out.println(rs.nextSolution().toString());
        }

        System.out.println("Done");
        System.exit(0);
    }
}
