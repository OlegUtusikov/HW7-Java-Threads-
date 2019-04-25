package ru.ifmo.rain.utusikov;

import info.kgeorgiy.java.advanced.concurrent.ListIP;
import info.kgeorgiy.java.advanced.concurrent.ScalarIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;
import org.jsoup.select.Collector;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public  class IterativeParallelism implements ListIP {
    ParallelMapper parallelMapper;
    IterativeParallelism() {
        parallelMapper = null;
    }

    IterativeParallelism(ParallelMapper parallelMapper) {
        this.parallelMapper = parallelMapper;
    }

    /**
     *
     * @param i count of threads {@link Thread}
     * @param list list of arguments {@link List}
     * @param comparator comparator for T {@link Comparator}
     * @param <T> tamplate parameter
     * @return maximum list from list
     * @throws InterruptedException when was error with threads
     */
    @Override
    public <T> T maximum(int i, List<? extends T> list, Comparator<? super T> comparator) throws InterruptedException {
        List<T> tmp = calc(i, list, stream -> stream.max(comparator).orElseThrow());
        return tmp.stream().max(comparator).orElseThrow();
    }

    /**
     *
     * @param i count of threads {@link Thread}
     * @param list list of arguments {@link List}
     * @param comparator comparator for T {@link Comparator}
     * @param <T> template parameter
     * @return minimum element from list
     * @throws InterruptedException when was error with threads
     */
    @Override
    public <T> T minimum(int i, List<? extends T> list, Comparator<? super T> comparator) throws InterruptedException {
        return maximum(i, list, Collections.reverseOrder(comparator));
    }

    /**
     *
     * @param i count of threads {@link Thread}
     * @param list list of arguments {@link List}
     * @param predicate function wich compare element with conditional {@link Predicate}
     * @param <T> template parameter
     * @return true when all elements match with predicate
     * @throws InterruptedException when was error with threads
     */
    @Override
    public <T> boolean all(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        List<Boolean> tmp = calc(i, list, stream -> stream.allMatch(predicate));
        return tmp.stream().allMatch(bool -> bool);
    }

    /**
     *
     * @param i count of threads {@link Thread}
     * @param list list of arguments {@link List}
     * @param predicate function which match elem with conditional {@link Predicate}
     * @param <T> template parameter
     * @return true, when some element match with predicate
     * @throws InterruptedException when was error with threads
     */
    @Override
    public <T> boolean any(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        return !all(i, list, predicate.negate());
    }

    /**
     *
     * @param i count of threads {@link Thread}
     * @param list list of arguments {@link List}
     * @return Joined list in String
     * @throws InterruptedException when was error with threads
     */
    @Override
    public String join(int i, List<?> list) throws InterruptedException {
        List<String> tmp = calc(i, list, stream -> stream.map(Object::toString).collect(Collectors.joining()));
        return String.join("", tmp);
    }

    /**
     *
     * @param i count of threads {@link Thread}
     * @param list list of arguments {@link List}
     * @param predicate return true when argument is good. {@link Predicate}
     * @param <T> template parameter
     * @return element which match with predicate function
     * @throws InterruptedException when was error with threads
     */
    @Override
    public <T> List<T> filter(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        List<List<T>> tmp = calc(i, list, stream -> stream.filter(predicate).collect(Collectors.toList()));
        return tmp.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    /**
     *
     * @param i count of Threads {@link Thread}
     * @param list list of arguments {@link List}
     * @param function function wich map element from list {@link Function}
     * @param <T>  template parameter
     * @param <U>  template parameter
     * @return List of mapped elements.
     * @throws InterruptedException when was error with threads
     */
    @Override
    public <T, U> List<U> map(int i, List<? extends T> list, Function<? super T, ? extends U> function) throws InterruptedException {
        List<List<U>> tmp = calc(i, list, stream -> stream.map(function).collect(Collectors.toList()), function);
        return tmp.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private <T, U, R> List<R> calc(int countOfThreads, List<T> list, Function<? extends Stream, R> func, Function<T, R> f) throws InterruptedException {
        if (countOfThreads <= 0) {
            throw new IllegalArgumentException("Count of threads mustn't be less zero!");
        }
        Objects.requireNonNull(list);
        List<R> result = new ArrayList<>();
        if (parallelMapper == null) {
            countOfThreads = Math.max(1, Math.min(list.size(), countOfThreads));
            result.addAll(Collections.nCopies(countOfThreads, null));
            int mainPath = list.size() / countOfThreads;
            int addPath = list.size() % countOfThreads;
            List<Thread> threads = new ArrayList<>();
            int l;
            int r = 0;
            for(int i = 0; i < countOfThreads; i++) {
                l = r;
                r = l + mainPath + (i < addPath ? 1 : 0);
                List<T> sublist = list.subList(l, r);
                int finalI = i;
                threads.add(new Thread(() -> result.set(finalI, func.apply(sublist.stream()))));
                threads.get(i).start();
            }
            for(Thread thread : threads) {
                thread.join();
            }
        } else {
            result.addAll(parallelMapper.map(f, list));
        }
        return result;
    }
}