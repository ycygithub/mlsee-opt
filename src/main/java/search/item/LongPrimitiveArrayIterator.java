package search.item;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LongPrimitiveArrayIterator implements Iterator<Long> {
  private final long[] array;
  private int position;
  private final int max;

  /**
   * <p>
   * Creates an {@link LongPrimitiveArrayIterator} over an entire array.
   * </p>
   *
   * @param array array to iterate over
   */
  public LongPrimitiveArrayIterator(long[] array) {
    this.array = Preconditions.checkNotNull(array); // yeah, not going to copy the array here, for performance
    this.position = 0;
    this.max = array.length;
  }

  @Override
  public boolean hasNext() {
    return position < max;
  }

  @Override
  public Long next() {
    return nextLong();
  }

  public long nextLong() {
    if (position >= array.length) {
      throw new NoSuchElementException();
    }
    return array[position++];
  }

  public long peek() {
    if (position >= array.length) {
      throw new NoSuchElementException();
    }
    return array[position];
  }

  /**
   * @throws UnsupportedOperationException
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }


  @Override
  public String toString() {
    return "LongPrimitiveArrayIterator";
  }

  public void reset() {
    this.position = 0;
  }

}