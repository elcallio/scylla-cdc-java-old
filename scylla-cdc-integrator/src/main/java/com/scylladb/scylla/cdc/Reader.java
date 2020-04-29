package com.scylladb.scylla.cdc;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.google.common.util.concurrent.Futures.addCallback;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;

public class Reader {
    private final Session session;
    private final LogSession logSession;
    private final PreparedStatement readEntries;

    Reader(Session session, LogSession logTable) {
        this.session = session;
        this.logSession = logTable;
        this.readEntries = session
                .prepare(String.format("SELECT * from %1$s.%2$s where %3$s >= ? and %3$s < ? and %4$s in ?", // order
                                                                                                             // by
                                                                                                             // %3$s",
                        quoteIfNecessary(logTable.getLogTable().getKeyspace().getName()),
                        quoteIfNecessary(logTable.getLogTable().getName()),
                        quoteIfNecessary(logTable.getTimeColumn().getName()),
                        quoteIfNecessary(logTable.getPkColumn().getName())));
    }

    public StreamPosition makeLimit() {
        return new StreamPosition(Instant.now().minus(logSession.getStreamPositionWindow()));
    }

    public CompletableFuture<LogResult> readStreams(Collection<Stream> streams, StreamPosition pos,
            StreamPosition limit) {
        if (limit.compareTo(pos) <= 0) {
            return CompletableFuture.completedFuture(new LogResult(Spliterators.emptySpliterator()));
        }

        BoundStatement statement = readEntries.bind(pos.getPosition(), limit.getPosition());
        statement.setList(2, streams.stream().map(Stream::getId).collect(toList()));
        statement.setFetchSize(logSession.getFetchSize());

        final CompletableFuture<LogResult> res = new CompletableFuture<>();

        addCallback(session.executeAsync(statement), new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                res.complete(new LogResult(new LogSpliterator(logSession, result)));
            }

            @Override
            public void onFailure(Throwable t) {
                res.completeExceptionally(t);
            }
        });
        return res;
    }

    public CompletableFuture<StreamPosition> readStreams(final Collection<Stream> streams, final StreamPosition pos,
            final StreamPosition limit, final Consumer<Event> consumer) {
        if (streams.size() > logSession.getMaxStreamsInQuery()) {
            List<Stream> tmp = streams.stream().limit(logSession.getMaxStreamsInQuery()).collect(toList());
            int n = tmp.size();
            return readStreams(tmp, pos, limit, consumer).thenCompose((lp) -> {
                return readStreams(streams.stream().skip(n).collect(toList()), pos, limit, consumer);
            });
        }
        return readStreams(streams, pos, limit).thenCompose((r) -> {
            r.stream().forEach(consumer);
            return completedFuture(limit);
        });
    }

    public CompletableFuture<StreamPosition> readAllStreams(StreamPosition pos, StreamPosition limit,
            final Consumer<Event> consumer) {
        SortedMap<Date, Set<Stream>> map = logSession.getStreams().streams(pos).stream()
                .collect(groupingBy(Stream::getTimestamp, TreeMap::new, toSet()));

        CompletableFuture<StreamPosition> f = completedFuture(limit);

        for (Set<Stream> set : map.values()) {
            f = f.thenCompose((p) -> readStreams(set, pos, limit, consumer));
        }

        return f;
    }
}

class ResultsSpliterator extends AbstractSpliterator<Spliterator<Event>> {
    private final LogSession session;
    private final Spliterator<ResultSet> resultSets;

    public ResultsSpliterator(LogSession session, long est, int additionalCharacteristics,
            Spliterator<ResultSet> resultSets) {
        super(est, additionalCharacteristics);
        this.session = session;
        this.resultSets = resultSets;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Spliterator<Event>> action) {
        return resultSets.tryAdvance((r) -> {
            action.accept(new LogSpliterator(session, r));
        });
    }
}

class FutureSplitIterator<T> extends AbstractSpliterator<T> {
    private List<CompletableFuture<Spliterator<T>>> futures;
    private Spliterator<T> current;

    public FutureSplitIterator(List<CompletableFuture<Spliterator<T>>> futures) {
        super(1, 0);
        this.futures = futures;
    }

    @Override
    public Spliterator<T> trySplit() {
        int n = futures.size() / 2;
        if (n == 0) {
            return null;
        }
        List<CompletableFuture<Spliterator<T>>> l1 = futures.subList(0, n);
        List<CompletableFuture<Spliterator<T>>> l2 = futures.subList(n, futures.size());

        this.futures = l1;

        return new FutureSplitIterator<T>(l2);
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        while (current == null || !current.tryAdvance(action)) {
            try {
                current = null;

                for (Future<Spliterator<T>> f : futures) {
                    if (f.isDone()) {
                        current = f.get();
                        futures.remove(f);
                        break;
                    }
                }

                // bah. just wait for first one.
                if (current == null && !futures.isEmpty()) {
                    current = futures.remove(0).get();
                }
                if (current == null) {
                    return false;
                }
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }
}

class LogSpliterator extends AbstractSpliterator<Event> {
    private final LogSession session;
    private final ResultSet resultSet;
    private final int refetch;

    private UUID last = null;
    private Event event = null;

    public LogSpliterator(LogSession session, ResultSet result) {
        super(result.getAvailableWithoutFetching() / 3, 0);
        this.session = session;
        this.resultSet = result;
        this.refetch = session.getRefetchSize();
    }

    @Override
    public long estimateSize() {
        long n = event != null ? 1 : 0;
        n += resultSet.getAvailableWithoutFetching() / 3;
        return n;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Event> action) {
        Event next = null;
        while (!resultSet.isExhausted() && next == null) {
            if (resultSet.getAvailableWithoutFetching() == refetch) {
                resultSet.fetchMoreResults();
            }

            Row r = resultSet.one();
            UUID timestamp = r.getUUID(session.getTimeColumnIndex());

            if (!timestamp.equals(last)) {
                next = event;
                event = new Event(timestamp);
                last = timestamp;
            }

            event.addRow(session, r);
        }

        if (next != null) {
            action.accept(next);
            return true;
        }

        if (event != null) {
            action.accept(event);
            event = null;
            return true;
        }

        return false;
    }

}
