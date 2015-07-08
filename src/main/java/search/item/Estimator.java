package search.item;

public interface Estimator<T> {

    double estimate(T thing) throws Exception;

}
