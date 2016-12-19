package org.linkeddatafragments.model;


import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.linkeddatafragments.client.LinkedDataFragmentsClient;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by ldevocht on 4/29/14.
 */
public class ExtendedTripleIteratorLDF implements ExtendedIterator<Triple> {
    protected ExtendedIterator<Triple> triples;
    protected LinkedDataFragmentIterator ldfIterator;
    protected Future<LinkedDataFragment> ldf;
    protected LinkedDataFragmentsClient ldfClient;

    public ExtendedTripleIteratorLDF(LinkedDataFragmentsClient ldfClient, Future<LinkedDataFragment> ldf) {
        this.ldf = ldf;
        this.ldfClient = ldfClient;
    }

    public ExtendedTripleIteratorLDF(LinkedDataFragmentsClient ldfClient, LinkedDataFragment ldf) {
        triples = ldf.getTriples();
        ldfIterator = LinkedDataFragmentIterator.create(ldf, ldfClient);
    }

    public static ExtendedIterator<Triple> create(LinkedDataFragmentsClient ldfClient, Future<LinkedDataFragment> ldf) {
        return new ExtendedTripleIteratorLDF(ldfClient, ldf);
    }

    public static ExtendedIterator<Triple> create(LinkedDataFragmentsClient ldfClient, LinkedDataFragment ldf) {
        return new ExtendedTripleIteratorLDF(ldfClient, ldf);
    }

    @Override
    public Triple removeNext() {
        waitForFragmentTriplesReady();
        return triples.removeNext();
    }

    @Override
    public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExtendedIterator<Triple> filterKeep(Predicate<Triple> predicate) {
        waitForFragmentTriplesReady();
        return triples.filterKeep(predicate);
    }

    @Override
    public ExtendedIterator<Triple> filterDrop(Predicate<Triple> predicate) {
        waitForFragmentTriplesReady();
        return triples.filterDrop(predicate);
    }

    @Override
    public <U> ExtendedIterator<U> mapWith(Function<Triple, U> function) {
        waitForFragmentTriplesReady();
        return triples.mapWith(function);
    }

    @Override
    public List<Triple> toList() {
        waitForFragmentTriplesReady();
        return triples.toList();
    }

    @Override
    public Set<Triple> toSet() {
        waitForFragmentTriplesReady();
        return triples.toSet();
    }

    @Override
    public void close() {
        waitForFragmentTriplesReady();
        triples.close();
    }

    @Override
    public boolean hasNext() {
        waitForFragmentTriplesReady();
        Boolean hasNext = triples.hasNext();
        if(!hasNext) {
            if(ldfIterator.hasNext()) {
                triples = ldfIterator.next().getTriples();
                if(triples.hasNext()) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return hasNext;
        }
    }

    @Override
    public Triple next() {
        waitForFragmentTriplesReady();
        Boolean hasNext = triples.hasNext();
        if(!hasNext) {
            if(ldfIterator.hasNext()) {
                triples = ldfIterator.next().getTriples();
                if(triples.hasNext()) {
                    return triples.next();
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else {
            return triples.next();
        }
    }

    @Override
    public void remove() {

    }

    private void waitForFragmentTriplesReady() {
        try {
            //available.acquire();
            if (ldfIterator == null) {
                //System.out.println("Waiting for iterator to be ready " + ldf.hashCode());
                ldfIterator = LinkedDataFragmentIterator.create(this.ldf, this.ldfClient);
                triples = ldfIterator.first();
                //System.out.println("ready " + this.ldf.hashCode());
            }
            //available.release();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
