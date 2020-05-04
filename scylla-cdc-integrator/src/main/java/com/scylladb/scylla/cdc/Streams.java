package com.scylladb.scylla.cdc;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
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

/**
 * Represents the available cdc streams in a scylla cluster. 
 * 
 * This includes both active and expired streams. Note that an 
 * expired stream can still receive writes for a short time 
 * after being replaced. Thus one must use a confidence interval
 * in sifting out streams that are considered consumed.  
 * 
 * The stream set is updated on cluster topology changes.
 *  
 * @author calle
 *
 */
public class Streams {
    private final Session session;
    private final PreparedStatement statement;

    public Streams(Session session) {
        this.session = session;
        this.statement = session
                .prepare("SELECT time, description, expired FROM system_distributed.cdc_topology_description");
        
        // listen for cluster changes. 
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

    /**
     * Sift out streams that has not expired prior to the 
     * threshold time. 
     *  
     * @param threshold - watermark timestamp
     * @return a list of streams (in ascending creation order)
     */
    public List<? extends Stream> streams(Date threshold) {
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

    /**
     * All streams since the beginning of time 
     *  
     * @param threshold - watermark timestamp
     * @return a list of streams (in ascending creation order)
     */
    public List<? extends Stream> streams() {
        return streams(new Date(0));
    }

    /** @see streams(Date) */
    public List<? extends Stream> streams(Instant threshold) {
        return streams(Date.from(threshold));
    }

    /** @see streams(Date) */
    public List<? extends Stream> streams(StreamPosition pos) {
        return streams(Instant.ofEpochMilli(UUIDs.unixTimestamp(pos.getPosition())));
    }

    /**
     * Sift out streams that has not expired prior to <code>delta</code>
     * units of time before now (wall clock) 
     *  
     * @param delta time units before now
     * @param unit unit designator
     * @return a list of streams (in ascending creation order)
     */
    public List<? extends Stream> streams(long delta, TemporalUnit unit) {
        Instant i = Instant.now();
        i.minus(delta, unit);
        return streams(i);
    }
}
