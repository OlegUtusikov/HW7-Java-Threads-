package ru.ifmo.rain.utusikov;

import info.kgeorgiy.java.advanced.concurrent.ListIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IterativeParallelism implements ListIP {
    private ParallelMapper mapper;

    /**
     * Default constructor.
     * Threads will create for each call methods.
     */
    public IterativeParallelism() {
        mapper = null;
    }

    /**
     * Constructor this thread pool.
     * Threads takes from {@code mapper} and doesn't create here.
     *
     * @param mapper thread pool
     */
    public IterativeParallelism(ParallelMapper mapper) {
        this.mapper = mapper;
    }

    private <T> List<List<? extends T>> getSublists(int threads, List<? extends T> list) {
        if (threads <= 0) {
            throw new IllegalArgumentException("Count of threads should be >= 1, but was: " + threads);
        }
        List<List<? extends T>> result = new ArrayList<>();
        threads = Math.max(1, Math.min(list.size(), threads));
        int mainPath = list.size() / threads;
        int addPath = list.size() % threads;
        int l;
        int r = 0;
        for(int i = 0; i < threads; i++) {
            l = r;
            r = Math.min(list.size(), l + mainPath + (i < addPath ? 1 : 0));
            result.add(list.subList(l, r));
        }
        return result;
    }

    private <T, U, R> R result(int threads, List<? extends T> list,
                               Function<List<? extends T>, ? extends U> func,
                               Function<List<? extends U>, ? extends R> concat) throws InterruptedException {
        var lists = getSublists(threads, list);
        final List<U> result = new ArrayList<>();
        List<Thread> workers = new ArrayList<>();
        if (mapper == null) {
            result.addAll(Collections.nCopies(lists.size(), null));
            for(int i = 0; i < lists.size(); i++) {
                final int finalI = i;
                final var temp = lists.get(finalI);
                workers.add(new Thread(() -> result.set(finalI, func.apply(temp))));
                workers.get(finalI).start();
            }
            for(Thread thread : workers) {
                try {
                    thread.join();
                }catch (InterruptedException ignored) {
                }
            }
        } else {
            result.addAll(mapper.map(func, lists));
        }

        return concat.apply(result);
    }

    /**
     * Find and return maximum element by comparator
     *
     * @param threads    number or concurrent threads.
     * @param values     values elements for checking.
     * @param comparator value comparator.
     * @param <T>        type of elements.
     * @return maximum element
     * @throws InterruptedException if threads error happened
     */
    @Override
    public <T> T maximum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
        Function<List<? extends T>, ? extends T> max = (list) -> list.stream().max(comparator).orElse(null);
        return result(threads, values, max, max);
    }

    /**
     * Find and return minimum element by comparator
     *
     * @param threads    number or concurrent threads.
     * @param values     values elements for checking.
     * @param comparator value comparator.
     * @param <T>        type of elements.
     * @return minimum element
     * @throws InterruptedException if threads error happened
     */
    @Override
    public <T> T minimum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
        return maximum(threads, values, Collections.reverseOrder(comparator));
    }

    /**
     * Check all elements for matching predicate
     *
     * @param threads   number or concurrent threads.
     * @param values    {@link List} elements for checking
     * @param predicate test predicate.
     * @param <T>       type of elements
     * @return true if any element of values match test predicate, otherwise false
     * @throws InterruptedException if threads error happened
     */
    @Override
    public <T> boolean all(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        return result(threads, values,
                (list) -> list.stream().allMatch(predicate),
                (list) -> list.stream().allMatch(b -> b));
    }

    /**
     * Check elements for matching predicate
     *
     * @param threads   number or concurrent threads.
     * @param values    {@link List} elements for checking
     * @param predicate test predicate.
     * @param <T>       type of elements
     * @return true if any element of values match test predicate, otherwise false
     * @throws InterruptedException if threads error happened
     */
    @Override
    public <T> boolean any(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        return !all(threads, values, predicate.negate());
    }

    /**
     * Concatenate string-value of all elements in list
     *
     * @param threads number or concurrent threads.
     * @param values  {@link List} elements for joining.
     * @return {@link String} contains all elements in list.
     * @throws InterruptedException if threads error happened.
     */
    @Override
    public String join(int threads, List<?> values) throws InterruptedException {
        return result(threads, values,
                (list) -> list.stream().map(Object::toString).collect(Collectors.joining()),
                (list) -> String.join("", list));
    }

    /**
     * Generate and return {@link List} of initial elements
     * which satisfy the {@code predicate}.
     *
     * @param threads   number or concurrent threads.
     * @param values    {@link List} elements for filtering.
     * @param predicate condition for filtering.
     * @param <T>       type of elements.
     * @return {@link List} of initial elements which satisfy the {@code predicate}.
     * @throws InterruptedException if threads error happened.
     */
    @Override
    public <T> List<T> filter(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        return result(threads, values,
                (list) -> list.stream().filter(predicate).collect(Collectors.toList()),
                (list) -> list.stream().flatMap(List::stream).collect(Collectors.toList()));
    }

    /**
     * @param threads number or concurrent threads.
     * @param values  {@link List} elements for mapping.
     * @param f       {@link Function} for mapping
     * @param <T>     type of elements in {@code values}
     * @param <U>     function result type
     * @return {@link List} of initial elements which mapped by {@code f}.
     * @throws InterruptedException if threads error happened
     */
    @Override
    public <T, U> List<U> map(int threads, List<? extends T> values, Function<? super T, ? extends U> f) throws InterruptedException {
        return result(threads, values,
                (list) -> list.stream().map(f).collect(Collectors.toList()),
                (list) -> list.stream().flatMap(List::stream).collect(Collectors.toList()));
    }
}