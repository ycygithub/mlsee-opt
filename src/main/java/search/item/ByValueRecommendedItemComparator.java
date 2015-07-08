package search.item;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Defines a natural ordering from most-preferred item (highest value) to least-preferred.
 */
public final class ByValueRecommendedItemComparator implements Comparator<RecommendedItem>, Serializable {

  private static final Comparator<RecommendedItem> INSTANCE = new ByValueRecommendedItemComparator();

  public static Comparator<RecommendedItem> getInstance() {
    return INSTANCE;
  }

  @Override
  public int compare(RecommendedItem o1, RecommendedItem o2) {
    double value1 = o1.getValue();
    double value2 = o2.getValue();
    return value1 > value2 ? -1 : value1 < value2 ? 1 : 0;
  }

}
