package search.item;

public interface IDRescorer {

    /**
     * @param id  ID of thing (user, item, etc.) to rescore
     * @param originalScore original score
     * @return modified score, or {@link Double#NaN} to indicate that this should be excluded entirely
     */
    double rescore(long id, double originalScore);

    /**
     * Returns {@code true} to exclude the given thing.
     * @param id ID of thing (user, item, etc.) to rescore
     * @return {@code true} to exclude, {@code false} otherwise
     */
    boolean isFiltered(long id);

}
