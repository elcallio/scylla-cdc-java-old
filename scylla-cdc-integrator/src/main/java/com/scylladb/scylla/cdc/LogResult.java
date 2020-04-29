package com.scylladb.scylla.cdc;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LogResult {

    private StreamPosition next;
    private Spliterator<Event> events;
    private Event event;

    LogResult(Spliterator<Event> events) {
        this.events = events;
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
