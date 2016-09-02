package org.linkeddatafragments.solver;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.*;
import org.apache.jena.sparql.util.Context;

/**
 * Created by ldevocht on 9/2/16.
 */
public class LDFOptimize implements Rewrite {

    public static Optimize.RewriterFactory factory = context1 -> new LDFOptimize(context1);

    public static void register() {
        Optimize.setFactory(factory);
    }

    protected Context context;

    protected boolean arqOptimization = false;

    public LDFOptimize(Context context) {
        this(context, true);
    }

    public LDFOptimize(Context context, boolean arqOptimization) {
        this.context = context;
        this.arqOptimization = arqOptimization;
        // this.context.set(ARQ.optFilterPlacement, false);
    }

    @Override
    public Op rewrite(Op rewrite) {
        Op op = rewrite;
        // taken from ARQ optimize
        // Prepare expressions.
        OpWalker.walk(op, new OpVisitorExprPrepare(context));

        // Need to allow subsystems to play with this list.
        if (context.isTrueOrUndef(ARQ.propertyFunctions))
            op = apply("Property Functions",
                    new TransformPropertyFunction(context), op);

        if (context.isTrueOrUndef(ARQ.optFilterConjunction))
            op = apply("filter conjunctions to ExprLists",
                    new TransformFilterConjunction(), op);

        if (context.isTrueOrUndef(ARQ.optFilterExpandOneOf))
            op = apply("Break up IN and NOT IN", new TransformExpandOneOf(), op);

        // Find joins/leftJoin that can be done by index joins (generally
        // preferred as fixed memory overhead).
        op = apply("Join strategy", new TransformJoinStrategy(), op);

        // TODO Improve filter placement to go through assigns that have
        // no effect.  Do this before filter placement and other sequence
        // generating transformations or improve to place in a sequence.

        if (context.isTrueOrUndef(ARQ.optFilterEquality)) {
            op = apply("Filter Equality", new TransformFilterEquality(), op);
        }

        if (context.isTrueOrUndef(ARQ.optFilterDisjunction))
            op = apply("Filter Disjunction", new TransformFilterDisjunction(), op);

        op = apply("Path flattening", new TransformPathFlattern(), op);
        // Mark

        return op;
    }

    public static Op apply(String label, Transform transform, Op op) {
        Op op2 = Transformer.transformSkipService(transform, op);

        return op2;
    }
}
