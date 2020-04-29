package com.scylladb.scylla.cdc;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class Streams {
    private final Session session;
    private final PreparedStatement statement;

    public Streams(Session session) {
        this.session = session;
        this.statement = session
                .prepare("SELECT time, description, expired FROM system_distributed.cdc_topology_description");
        session.getCluster().register(new Host.StateListener() {

            @Override
            public void onUp(Host host) {
                rebuild();
            }

            @Override
            public void onUnregister(Cluster cluster) {
                rebuild();
            }

            @Override
            public void onRemove(Host host) {
                rebuild();
            }

            @Override
            public void onRegister(Cluster cluster) {
                rebuild();
            }

            @Override
            public void onDown(Host host) {
                rebuild();
            }

            @Override
            public void onAdd(Host host) {
                rebuild();
            }
        });
        try {
            rebuild().get();
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
        }
    }

    private SortedMap<Date, List<Stream>> allStreams = Collections.emptySortedMap();

    private Future<Void> rebuild() {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        Futures.addCallback(session.executeAsync(statement.bind()), new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(ResultSet result) {
                SortedMap<Date, List<Stream>> map = new TreeMap<>(allStreams);
                List<Row> rows = result.all();
                for (Row r : rows) {
                    Date timestamp = r.getTimestamp(0);
                    Date expired = r.getTimestamp(2);

                    List<Stream> streams = map.computeIfAbsent(timestamp, (d) -> {
                        List<Stream> res = new ArrayList<>();
                        for (TupleValue v : r.getList(1, TupleValue.class)) {
                            for (ByteBuffer b : v.getList(1, ByteBuffer.class)) {
                                res.add(new Stream(timestamp, b));
                            }
                        }
                        return res;
                    });

                    if (expired != null) {
                        for (Stream s : streams) {
                            s.expire(expired);
                        }
                    }
                }

                allStreams = map;

                f.complete(null);
            }

            @Override
            public void onFailure(Throwable t) {
                f.completeExceptionally(t);
            }

        });

        return f;
    }

    public Collection<? extends Stream> streams(Date threshold) {
        return allStreams.values().stream().collect(ArrayList::new, (res, streams) -> {
            if (streams.isEmpty()) {
                // should not happen, but...
                return;
            }
            Stream s = streams.get(0);
            if (!s.isExpired() || threshold.compareTo(s.getExpired()) <= 0) {
                res.addAll(streams);
            }
        }, ArrayList::addAll);
    }

    public Collection<? extends Stream> streams() {
        return streams(new Date(0));
    }

    public Collection<? extends Stream> streams(Instant threshold) {
        return streams(Date.from(threshold));
    }

    public Collection<? extends Stream> streams(StreamPosition pos) {
        return streams(Instant.ofEpochMilli(UUIDs.unixTimestamp(pos.getPosition())));
    }

    public Collection<? extends Stream> streams(long newerThanDelta, TemporalUnit unit) {
        Instant i = Instant.now();
        i.minus(newerThanDelta, unit);
        return streams(i);
    }
}
