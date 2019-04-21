package ru.ifmo.rain.utusikov;

import info.kgeorgiy.java.advanced.concurrent.ListIP;
import info.kgeorgiy.java.advanced.concurrent.ScalarIP;
import org.jsoup.select.Collector;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public  class IterativeParallelism implements ListIP {

    @Override
    public <T> T maximum(int i, List<? extends T> list, Comparator<? super T> comparator) throws InterruptedException {
        List<T> tmp = calc(i, list, stream -> stream.max(comparator).orElseThrow());
        return tmp.stream().max(comparator).orElseThrow();
    }

    @Override
    public <T> T minimum(int i, List<? extends T> list, Comparator<? super T> comparator) throws InterruptedException {
        return maximum(i, list, Collections.reverseOrder(comparator));
    }

    @Override
    public <T> boolean all(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        List<Boolean> tmp = calc(i, list, stream -> stream.allMatch(predicate));
        return tmp.stream().allMatch(bool -> bool);
    }

    @Override
    public <T> boolean any(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        return !all(i, list, predicate.negate());
    }

    @Override
    public String join(int i, List<?> list) throws InterruptedException {
        List<String> tmp = calc(i, list, stream -> stream.map(Object::toString).collect(Collectors.joining()));
        return String.join("", tmp);
    }

    @Override
    public <T> List<T> filter(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        List<List<T>> tmp = calc(i, list, stream -> stream.filter(predicate).collect(Collectors.toList()));
        return tmp.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public <T, U> List<U> map(int i, List<? extends T> list, Function<? super T, ? extends U> function) throws InterruptedException {
        List<List<U>> tmp = calc(i, list, stream -> stream.map(function).collect(Collectors.toList()));
        return tmp.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private <T, R> List<R> calc(int countOfThreads, List<T> list, Function<Stream<T>, R> func) throws InterruptedException {
        if (countOfThreads <= 0) {
            throw new IllegalArgumentException("Count of threads mustn't be less zero!");
        }
        Objects.requireNonNull(list);
        countOfThreads = Math.max(1, Math.min(list.size(), countOfThreads));
        List<R> result = new ArrayList<>(Collections.nCopies(countOfThreads, null));
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
        return result;
    }
}