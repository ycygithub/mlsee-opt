package search.item;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.*;

public class TopItems {
  private static final long[] NO_IDS = new long[0];

  private TopItems() {
  }

  public static List<RecommendedItem> getTopItems(int howMany,
                                                  Iterator<Long> possibleItemIDs,
                                                  IDRescorer rescorer,
                                                  Estimator<Long> estimator) throws Exception {
    Preconditions.checkArgument(possibleItemIDs != null, "possibleItemIDs is null");
    Preconditions.checkArgument(estimator != null, "estimator is null");

    Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(howMany + 1,
      Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
    boolean full = false;
    double lowestTopValue = Double.NEGATIVE_INFINITY;
    while (possibleItemIDs.hasNext()) {
      long itemID = possibleItemIDs.next();
      if (rescorer == null || !rescorer.isFiltered(itemID)) {
        double preference;
        preference = estimator.estimate(itemID);

        double rescoredPref = rescorer == null ? preference : rescorer.rescore(itemID, preference);
        if (!Double.isNaN(rescoredPref) && (!full || rescoredPref > lowestTopValue)) {
          RecommendedItem recommendedItem = new GenericRecommendedItem(itemID, new Double(preference).floatValue());
          topItems.add(recommendedItem);
          if (full) {
            topItems.poll();
          } else if (topItems.size() > howMany) {
            full = true;
            topItems.poll();
          }
          lowestTopValue = topItems.peek().getValue();
        }
      }
    }
    int size = topItems.size();
    if (size == 0) {
      return Collections.emptyList();
    }
    List<RecommendedItem> result = Lists.newArrayListWithCapacity(size);
    result.addAll(topItems);
    Collections.sort(result, ByValueRecommendedItemComparator.getInstance());
    return result;
  }

}
