package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> fieldCntMap;
    private static final StringField NO_GROUPING_FIELD = new StringField("NO_GROUPING_FIELD", 20);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Wrong operation type! Expect COUNT, get " + what);
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.fieldCntMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            fieldCntMap.put(NO_GROUPING_FIELD, fieldCntMap.getOrDefault(NO_GROUPING_FIELD, 0) + 1);
        }
        else {
            fieldCntMap.put(tup.getField(gbfield), fieldCntMap.getOrDefault(tup.getField(gbfield), 0) + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc = gbfield == NO_GROUPING ? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        List<Tuple> tupleList = new ArrayList<>();
        for (Field key : fieldCntMap.keySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            if (gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(fieldCntMap.get(key)));
            }
            else {
                tuple.setField(0, key);
                tuple.setField(1, new IntField(fieldCntMap.get(key)));
            }
            tupleList.add(tuple);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }
}
