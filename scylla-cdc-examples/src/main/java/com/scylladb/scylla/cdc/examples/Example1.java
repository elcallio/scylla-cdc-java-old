package com.scylladb.scylla.cdc.examples;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.scylladb.scylla.cdc.StreamPosition.INITIAL;
import static net.sourceforge.argparse4j.ArgumentParsers.newFor;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.table.AbstractTableModel;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.reflect.TypeToken;
import com.scylladb.scylla.cdc.Event;
import com.scylladb.scylla.cdc.LogSession;
import com.scylladb.scylla.cdc.Reader;
import com.scylladb.scylla.cdc.RowImage;
import com.scylladb.scylla.cdc.StreamPosition;

import dnl.utils.text.table.TextTable;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Example1 {
    private static final ProtocolVersion PROTOCOL_VERSION = V4;

    @SuppressWarnings("serial")
    private static class LogModel extends AbstractTableModel {
        private final List<RowImage> rows;

        public LogModel(List<Event> events) {
            this.rows = new ArrayList<RowImage>(events.size() * 3);
            events.forEach((e) -> rows.addAll(e.getRows()));
        }

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return 4 + (rows.isEmpty() ? 0 : rows.get(0).getColumns().size());
        }

        @Override
        public String getColumnName(int column) {
            switch (column) {
            case 0:
                return "timestamp";
            case 1:
                return "op";
            case 2:
                return "seq";
            case 3:
                return "ttl";
            default:
                break;
            }

            return rows.get(0).getColumns().get(column - 4).getName();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            RowImage r = rows.get(rowIndex);

            switch (columnIndex) {
            case 0:
                return r.getTimeUUID();
            case 1:
                return r.getType();
            case 2:
                return r.getBatchSequence();
            case 3:
                return r.getTTL();
            default:
                break;
            }

            int i = columnIndex - 4;

            String s = "";
            boolean isDeleted = r.isDeleted(i);
            DataType t = r.getColumns().get(i).getType();

            if (isDeleted) {
                s = "<null>";
            }
            if (!r.isNull(i)) {
                s = (t.isCollection() && !isDeleted ? " + " : "") + r.getObject(i).toString();
            }

            if (r.hasDeletedElements(i)) {
                s += " - " + r.getDeletedSetElements(i, (TypeToken<Object>) null).toString();
            }

            return s;
        }

    }

    public static void main(String[] args) {
        ArgumentParser parser = newFor("Example1").build().defaultHelp(true);

        parser.addArgument("-k", "--keyspace").help("Keyspace");
        parser.addArgument("-t", "--table").help("Table");
        parser.addArgument("-a", "--addresses").nargs("*").help("Initial node addresses");
        parser.addArgument("-i", "--interval").help("Polling delay/interval (seconds)").setDefault(5);
        parser.addArgument("-r", "--rows").help("Number of events per printout").setDefault(100);

        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        final String hosts[] = ns.<String> getList("addresses").stream().toArray(String[]::new);
        final String keyspace = ns.getString("keyspace");
        final String table = ns.getString("table");
        final int rows = ns.getInt("rows");
        final int wait = ns.getInt("interval");

        PoolingOptions poolingOptions = new PoolingOptions();

        int connections = 8;
        poolingOptions.setCoreConnectionsPerHost(LOCAL, Math.max(1, connections / 2));
        poolingOptions.setCoreConnectionsPerHost(REMOTE, Math.max(1, connections / 4));
        poolingOptions.setMaxConnectionsPerHost(LOCAL, connections);
        poolingOptions.setMaxConnectionsPerHost(REMOTE, Math.max(1, connections / 2));
        poolingOptions.setMaxRequestsPerConnection(LOCAL, 32768);
        poolingOptions.setMaxRequestsPerConnection(REMOTE, 2000);

        Cluster.Builder builder = builder().addContactPoints(hosts).withProtocolVersion(PROTOCOL_VERSION)
                /* .withCompression(LZ4) */.withPoolingOptions(poolingOptions)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .withQueryOptions(new QueryOptions().setDefaultIdempotence(true));

        Cluster cluster = builder.build();
        Session session = cluster.connect();

        LogSession logSession = LogSession.builder(session).withKeyspace(keyspace).withTable(table).build();

        Reader r = logSession.createReader();
        StreamPosition pos = INITIAL;
        PrintStream out = System.out;

        for (;;) {
            try {
                List<Event> events = new ArrayList<>();
                pos = r.readAllStreams(pos, r.makeLimit(), (event) -> {
                    events.add(event);
                    if (events.size() == rows) {
                        print(out, events);
                        events.clear();
                    }
                }).get();

                print(out, events);

                Thread.sleep(1000 * wait);
            } catch (InterruptedException | ExecutionException e) {
                out.format("Log retrieval failed: %s", e);
            }
        }
    }

    private static void print(PrintStream out, List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        LogModel m = new LogModel(events);
        new TextTable(m).printTable(out, 0);
    }
}
