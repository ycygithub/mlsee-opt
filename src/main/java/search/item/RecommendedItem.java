package search.item;

/**
 * <p>
 * Implementations encapsulate items that are recommended, and include the item recommended and a value
 * expressing the strength of the preference.
 * </p>
 */
public interface RecommendedItem {

  /** @return the recommended item ID */
  long getItemID();

  /**
   * <p>
   * A value expressing the strength of the preference for the recommended item. The range of the values
   * depends on the implementation. Implementations must use larger values to express stronger preference.
   * </p>
   *
   * @return strength of the preference
   */
  double getValue();

}
