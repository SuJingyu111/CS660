package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Field> resMap;
    private Map<Field, Integer> fieldCntMap;
    private static final IntField NO_GROUPING_FIELD = new IntField(-1);
    private Type valueType;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.resMap = new HashMap<>();
        this.fieldCntMap = new HashMap<>();
        this.valueType = Type.INT_TYPE;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            fieldCntMap.put(NO_GROUPING_FIELD, fieldCntMap.getOrDefault(NO_GROUPING_FIELD, 0) + 1);
            if (resMap.containsKey(NO_GROUPING_FIELD)) {
                resMap.put(NO_GROUPING_FIELD, aggregate(what, tup.getField(afield), resMap.get(NO_GROUPING_FIELD), NO_GROUPING_FIELD));
            }
            else {
                resMap.put(NO_GROUPING_FIELD, tup.getField(afield));
            }
        }
        else {
            Field key = tup.getField(gbfield);
            Field value = tup.getField(afield);
            fieldCntMap.put(key, fieldCntMap.getOrDefault(key, 0) + 1);
            if (resMap.containsKey(key)) {
                resMap.put(key, aggregate(what, value, resMap.get(key), key));
            }
            else {
                resMap.put(key, value);
            }
        }
    }

    private Field aggregate(Op what, Field value, Field prevAggregate, Field key) {
        if (what == Op.MAX) {
            return value.compare(Predicate.Op.GREATER_THAN_OR_EQ, prevAggregate) ? new IntField(((IntField)value).getValue())
                    : new IntField(((IntField)prevAggregate).getValue());
        }
        if (what == Op.MIN) {
            return value.compare(Predicate.Op.LESS_THAN_OR_EQ, prevAggregate) ? new IntField(((IntField)value).getValue())
                    : new IntField(((IntField)prevAggregate).getValue());
        }
        if (what == Op.SUM || what == Op.AVG) {
            return new IntField(((IntField)value).getValue() + ((IntField)prevAggregate).getValue());
        }
        if (what == Op.COUNT) {
            return new IntField((fieldCntMap.get(key)));
        }
        return null;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc = gbfield == NO_GROUPING ? new TupleDesc(new Type[]{valueType}) : new TupleDesc(new Type[]{gbfieldtype, valueType});
        List<Tuple> tupleList = new ArrayList<>();
        for (Field key : resMap.keySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            if (gbfield == NO_GROUPING) {
                if (what == Op.AVG) {
                    tuple.setField(0, new IntField(((IntField)resMap.get(key)).getValue() / fieldCntMap.get(key)));
                }
                else {
                    tuple.setField(0, new IntField(((IntField)resMap.get(key)).getValue()));
                }
            }
            else {
                if (what == Op.AVG) {
                    tuple.setField(0, key);
                    tuple.setField(1, new IntField(((IntField)resMap.get(key)).getValue() / fieldCntMap.get(key)));
                }
                else {
                    tuple.setField(0, key);
                    tuple.setField(1, new IntField(((IntField)resMap.get(key)).getValue()));
                }
            }
            tupleList.add(tuple);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }

}
