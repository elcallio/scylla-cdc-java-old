package com.scylladb.scylla.cdc;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;

/**
 * Pageable result set of CDC {@link Event}s.
 * 
 * @see ResultSet
 * @author calle
 *
 */
public class LogResult {
    private final StreamPosition next;
    private final Spliterator<Event> events;
    private Event event;

    LogResult(Spliterator<Event> events, StreamPosition next) {
        this.events = events;
        this.next = next;
    }

    public Event one() {
        event = null;
        if (!events.tryAdvance((e) -> event = e)) {
            return null;
        }
        return event;
    }

    public boolean isExhausted() {
        return events.estimateSize() == 0;
    }

    public Stream<Event> stream() {
        return StreamSupport.stream(events, false);
    }

    public Stream<Event> parallelStream() {
        return StreamSupport.stream(events, true);
    }

    public List<Event> all() {
        return stream().collect(toList());
    }

    public StreamPosition next() {
        return next;
    }
}
