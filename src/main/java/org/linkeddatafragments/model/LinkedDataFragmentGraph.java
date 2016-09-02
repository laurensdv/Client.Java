package org.linkeddatafragments.model;

import com.google.common.primitives.Ints;
import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.GraphMatcher;
import org.apache.jena.query.ARQ;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.ClosedException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.linkeddatafragments.client.LinkedDataFragmentsClient;
import org.linkeddatafragments.solver.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;

import java.util.concurrent.Future;

// TODO check if optimization is possible cfr
// http://parliament.semwebcentral.org/javadoc/com/bbn/parliament/jena/query/optimize/ReorderQueryIterTriplePattern.html#ReorderQueryIterTriplePattern(com.hp.hpl.jena.sparql.core.BasicPattern, com.hp.hpl.jena.sparql.engine.QueryIterator, com.hp.hpl.jena.sparql.engine.ExecutionContext)

public class LinkedDataFragmentGraph extends GraphBase {
    protected final LinkedDataFragmentsClient ldfClient;
    protected ReorderTransformation reorderTransform;
    protected LDFStatistics ldfStatistics;

    static {
        // Register OpExecutor
        QC.setFactory(ARQ.getContext(), OpExecutorLDF.opExecFactoryLDF);

        //Set Stagegenerator
        StageGenerator orig = (StageGenerator) ARQ.getContext()
                .get(ARQ.stageGenerator);
        StageGenerator generator = new LDFStageGenerator(orig);
        StageBuilder.setGenerator(ARQ.getContext(), generator);

        //Register Engine
        LinkedDataFragmentEngine.register();

        //Register optimizer
        LDFOptimize.register();
    }

    public LinkedDataFragmentGraph(String dataSource) {
          super();
          this.ldfClient = new LinkedDataFragmentsClient(dataSource);
          this.reorderTransform=new ReorderTransformationLDF(this);
          this.ldfStatistics = new LDFStatistics(this);  //must go after ldfClient created
    }

    /**
     Default implementation answers <code>true</code> iff this graph is the
     same graph as the argument graph.
     */
    @Override
    public boolean dependsOn( Graph other )
    { return ldfClient.getBaseFragment().getGraph() == other; }

    @Override
    public void add(Triple t) throws AddDeniedException {
        throw new UnsupportedOperationException();
    }

    /**
     Answer the capabilities of this graph; the default is an AllCapabilities object
     (the same one each time, not that it matters - Capabilities should be
     immutable).
     */
    @Override
    public Capabilities getCapabilities()
    {
        if (capabilities == null) capabilities = new LinkedDataFragmentGraphCapabilities();
        return capabilities;
    }

    @Async
    private Future<LinkedDataFragment> retrieveFragment(Triple m) throws Exception {
        LinkedDataFragment ldf = ldfClient.getFragment(ldfClient.getBaseFragment(), m);
        return new AsyncResult<>(ldf);
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple m) {
        try{
            //System.out.println("finding triple "+m.hashCode());
            Future<LinkedDataFragment> ldf = retrieveFragment(m);
            //System.out.println("triple retrieving"+m.hashCode());
            return ExtendedTripleIteratorLDF.create(ldfClient, ldf);
        } catch(Exception e) {
            e.printStackTrace();
            return WrappedIterator.emptyIterator(); //Do not block on error but return empty iterator
        }
    }

    public long getNodeCountInPosition(Node node, int position) throws Exception {
        return getCount(new Triple(node, Node.ANY, Node.ANY)).get();
    }

    @Async
    private Future<Long> retrieveFragmentCount(Triple m) {
        try{
            LinkedDataFragment ldf = ldfClient.getFragment(ldfClient.getBaseFragment(), m);
            Long count = ldf.getMatchCount();
            return new AsyncResult<>(count);
        } catch(Exception e) {
            return new AsyncResult<>(0L);
        }
    }

    public Future<Long> getCount(Triple m) {
        Future<Long> count = retrieveFragmentCount(m);
        return count;
    }

    public ReorderTransformation getReorderTransform() {
        return reorderTransform;
    }

    protected int graphBaseSize() {
        try{
            return Ints.checkedCast(ldfClient.getBaseFragment().getTriplesSize());
        } catch(IllegalArgumentException e) {
            return Integer.MAX_VALUE; //return a very high number
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        if(this.ldfStatistics == null) {
            this.ldfStatistics = new LDFStatistics(this);
        }
        return this.ldfStatistics;
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        throw new UnsupportedOperationException();
    }

    /**
     Answer true iff this graph is isomorphic to <code>g</code> according to
     the algorithm (indeed, method) in <code>GraphMatcher</code>.
     */
    @Override
    public boolean isIsomorphicWith( Graph g )
    { checkOpen();
        return g != null && GraphMatcher.equals(ldfClient.getBaseFragment().getGraph(), g); }

    /**
     Answer a human-consumable representation of this graph. Not advised for
     big graphs, as it generates a big string: intended for debugging purposes.
     */

    /**
     Utility method: throw a ClosedException if this graph has been closed.
     */
    protected void checkOpen()
    { if (closed) throw new ClosedException( "already closed", ldfClient.getBaseFragment().getGraph() ); }

    @Override public String toString()
    { return toString( (closed ? "closed " : ""), ldfClient.getBaseFragment().getGraph() ); }

    /**
     Answer a human-consumable representation of <code>that</code>. The
     string <code>prefix</code> will appear near the beginning of the string. Nodes
     may be prefix-compressed using <code>that</code>'s prefix-mapping. This
     default implementation will display all the triples exposed by the graph (ie
     including reification triples if it is Standard).
     */
    public static String toString( String prefix, Graph that )
    {
        PrefixMapping pm = that.getPrefixMapping();
        StringBuffer b = new StringBuffer( prefix + " {" );
        int count = 0;
        String gap = "";
        ClosableIterator<Triple> it = GraphUtil.findAll(that);
        while (it.hasNext() && count < TOSTRING_TRIPLE_LIMIT)
        {
            b.append( gap );
            gap = "; ";
            count += 1;
            b.append( it.next().toString( pm ) );
        }
        if (it.hasNext()) b.append( "..." );
        it.close();
        b.append( "}" );
        return b.toString();
    }

//    @Override
//    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
//        return find(triple);
//    }

}
