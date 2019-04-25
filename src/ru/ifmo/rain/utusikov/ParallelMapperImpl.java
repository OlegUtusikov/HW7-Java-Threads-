package ru.ifmo.rain.utusikov;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;
import static java.lang.Thread.interrupted;
import java.lang.Thread;

public class ParallelMapperImpl implements ParallelMapper {
    private List<Thread> threads;
    private Queue<Work> works;

    public ParallelMapperImpl(int threads) {
        this.threads = new ArrayList<>();
        this.works = new ArrayDeque<>();
        for(int i = 0; i < threads; i++) {
            this.threads.add(new Thread(() -> {
                try {
                    while(!interrupted())
                    exec();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));
            this.threads.get(i).start();
        }
    }

    private void exec() throws InterruptedException {
        Work work = null;
        synchronized (works) {
            while (works.isEmpty()) {
                wait();
            }
            work = works.poll();
        }
        work.task.run();
        work.inc.run();
    }

    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> function, List<? extends T> list) throws InterruptedException {
        ParallelList<R> parallelList = new ParallelList<>();
        for(int i = 0; i < list.size(); i++) {
            synchronized (works) {
                int finalI = i;
                works.add(new Work(() -> parallelList.set(finalI, function.apply(list.get(finalI))), parallelList::inc));
                works.notifyAll();
            }
        }
        return parallelList.getList();
    }

    @Override
    public void close() {
        for(Thread thread : threads) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class Work {
        Runnable task;
        Runnable inc;

        Work(Runnable task, Runnable inc) {
            this.task = task;
            this.inc = inc;
        }
    }

    public class ParallelList<T> extends  AbstractList<T> {
        private List<T> list;
        private int completed = 0;

        public ParallelList() {
            list = Collections.emptyList();
        }

        public ParallelList(Collection<T> collection) {
            list = new ArrayList<>(collection);
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public T get(int index) {
            return list.get(index);
        }

        public T set(int index, T value) {
            synchronized (this) {
                list.set(index, value);
            }
            return list.get(index);
        }

        void inc() {
            synchronized (this) {
                completed++;
                notifyAll();
            }
        }

        List<T> getList() throws InterruptedException {
            synchronized (this) {
                if (completed != list.size()) {
                    wait();
                }
            }
            return list;
        }
    }
}
