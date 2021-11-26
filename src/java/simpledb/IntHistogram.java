package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;

    private int min, max;

    private int bucketNum;

    private int sum;

    private int bucketLen;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.bucketNum = buckets;
        this.min = min;
        this.max = max;
        this.sum = 0;
        this.bucketLen = Math.max(1, (max - min + 1) / bucketNum);
        //System.out.println(min + " " + max + " " + bucketNum + " " + bucketLen);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        buckets[getIdx(v)]++;
        sum++;
    }

    private int getIdx(int v) {
        int idx = (v - min) / Math.max(1, bucketLen);
        if (idx >= bucketNum) {
            idx = bucketNum - 1;
        }
        else if (idx < 0) {
            idx = 0;
        }
        return idx;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double res = 0.0;
        if (op == Predicate.Op.EQUALS) {
            if (!(v > max || v < min)) {
                res = (buckets[getIdx(v)] / ((double)bucketLen)) / sum;
                //System.out.println(bucketLen);
                //System.out.println(res);
            }
        }
        else if (op == Predicate.Op.GREATER_THAN) {
            if (v < min) {
                v = min - 1;
            }
            if (v < max) {
                int idx = Math.max(0, getIdx(v + 1));
                int bucketBase = min + idx * bucketLen;
                for (int i = idx + 1; i < bucketNum; i++) {
                    res += buckets[i];
                }
                res += (double) (bucketBase + bucketLen - 1 - v) / bucketLen * buckets[idx];
                res /= sum;
            }
        }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v < min) {
                v = min;
            }
            if (v <= max) {
                int idx = Math.max(0, getIdx(v));
                int bucketBase = min + idx * bucketLen;
                for (int i = idx + 1; i < bucketNum; i++) {
                    res += buckets[i];
                }
                res += (double) (bucketBase + bucketLen - v) / bucketLen * buckets[idx];
                res /= sum;
            }
        }
        else if (op == Predicate.Op.LESS_THAN) {
            if (v > max) {
                v = max + 1;
            }
            if (v > min) {
                int idx = Math.max(0, getIdx(v - 1));
                int bucketBase = min + idx * bucketLen;
                for (int i = idx - 1; i >= 0; i--) {
                    res += buckets[i];
                }
                res += (double) (v - bucketBase) / bucketLen * buckets[idx];
                res /= sum;
            }
        }
        else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v > max) {
                v = max;
            }
            if (v >= min) {
                int idx = Math.max(0, getIdx(v));
                int bucketBase = min + idx * bucketLen;
                for (int i = idx - 1; i >= 0; i--) {
                    res += buckets[i];
                }
                res += (double) (v + 1 - bucketBase) / bucketLen * buckets[idx];
                res /= sum;
            }
        }
        else if (op == Predicate.Op.NOT_EQUALS) {
            if (v > max || v < min) {
                res = 1.0;
            }
            else {
                res = ((double) sum - (buckets[getIdx(v)] / ((double)bucketLen))) / sum;
            }
        }
        return res;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double res = 0.0;
        for (int i = 0; i < bucketNum; i++) {
            res += (double) buckets[i] / sum;
        }
        res /= (max - min);
        return res;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
