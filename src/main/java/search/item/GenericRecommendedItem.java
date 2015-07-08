package search.item;

import com.google.common.base.Preconditions;
import org.apache.mahout.common.RandomUtils;

import java.io.Serializable;

/**
 * <p>
 * A simple implementation of {@link RecommendedItem}.
 * </p>
 */
public final class GenericRecommendedItem implements RecommendedItem, Serializable {

  private final long itemID;
  private final double value;

  /**
   * @throws IllegalArgumentException if item is null or value is NaN
   */
  public GenericRecommendedItem(long itemID, double value) {
    Preconditions.checkArgument(!Double.isNaN(value), "value is NaN");
    this.itemID = itemID;
    this.value = value;
  }

  @Override
  public long getItemID() {
    return itemID;
  }

  @Override
  public double getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "RecommendedItem[item:" + itemID + ", value:" + value + ']';
  }

  @Override
  public int hashCode() {
    return (int) itemID ^ RandomUtils.hashDouble(value);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GenericRecommendedItem)) {
      return false;
    }
    RecommendedItem other = (RecommendedItem) o;
    return itemID == other.getItemID() && value == other.getValue();
  }

}
