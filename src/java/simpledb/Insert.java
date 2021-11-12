package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator chid;
    private int tableId;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.chid = child;
        this.tableId = tableId;

        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(types);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.chid.open();
    }

    public void close() {
        // some code goes here
        this.chid.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.chid.close();
        this.chid.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        while (this.chid.hasNext()) {
            try {
                count++;
                bufferPool.insertTuple(this.t, this.tableId, this.chid.next());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple tup = new Tuple(this.tupleDesc);
        tup.setField(0, new IntField(count));
        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = chid;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.chid = children[0];
    }
}
