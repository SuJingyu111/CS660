package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator chid;
    private TupleDesc tupleDesc;
    private Tuple tup;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.chid = child;

        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(types);

        this.tup = null;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.chid.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.chid.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        if (this.tup != null) {
            return null;
        }
        while (this.chid.hasNext()) {
            try {
                count++;
                bufferPool.deleteTuple(this.t, this.chid.next());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tup = new Tuple(this.tupleDesc);
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
