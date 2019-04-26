package ru.ifmo.rain.utusikov;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import java.lang.Thread;

public class ParallelMapperImpl implements ParallelMapper {
    private final List<Thread> threads;
    private final Queue<Work> works;

    public ParallelMapperImpl(int threads) {
        this.threads = new ArrayList<>();
        this.works = new ArrayDeque<>();
        for(int i = 0; i < threads; i++) {
            this.threads.add(new Thread(() -> {
                try {
                    while(!interrupted()) {
                        exec();
                    }
                } catch (InterruptedException ignored) {
                    currentThread().interrupt();
                }
            }));
            this.threads.get(i).start();
        }
    }

    private void exec() throws InterruptedException {
        Work work;
        synchronized (works) {
            while (works.isEmpty()) {
                works.wait();
            }
            work = works.poll();
        }
        work.task.run();
        work.inc.run();
    }

    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> function, List<? extends T> list) throws InterruptedException {
        ParallelList<R> parallelList = new ParallelList<>(Collections.nCopies(list.size(), null));
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
            } catch (InterruptedException ignored) {
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
        private final List<T> list;
        private int completed = 0;

        public ParallelList() {
            this(Collections.emptyList());
        }

        ParallelList(Collection<T> collection) {
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
            list.set(index, value);
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
