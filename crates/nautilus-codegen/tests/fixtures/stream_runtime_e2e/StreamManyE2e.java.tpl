package com.acme.db.e2e;

import com.acme.db.client.Nautilus;
import com.acme.db.client.NautilusOptions;
import com.acme.db.dsl.SortOrder;
import com.acme.db.internal.BaseNautilusClient;
import com.acme.db.model.User;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StreamManyE2e {
    private StreamManyE2e() {
    }

    public static void main(String[] args) throws Exception {
        NautilusOptions options = new NautilusOptions().maxConnections(1);
        String enginePath = System.getenv("NAUTILUS_BIN");
        if (enginePath != null && !enginePath.isBlank()) {
            options.enginePath(enginePath);
        }

        try (Nautilus db = new Nautilus(options)) {
            List<String> seen = new ArrayList<>();
            try (Stream<User> stream = db.user().streamMany(find -> find
                .orderBy(order -> order.id(SortOrder.ASC))
                .chunkSize(1)
            )) {
                Iterator<User> iterator = stream.iterator();
                while (iterator.hasNext() && seen.size() < __BREAK_AFTER__) {
                    seen.add(iterator.next().name());
                }
            }

            Thread.sleep(100L);

            List<User> follow = db.user().findMany(find -> find
                .orderBy(order -> order.id(SortOrder.ASC))
                .take(5)
            );
            List<User> tail = db.user().findMany(find -> find
                .orderBy(order -> order.id(SortOrder.ASC))
                .skip(__TAIL_SKIP__)
                .take(1)
            );

            Field partialDataField = BaseNautilusClient.class.getDeclaredField("partialData");
            partialDataField.setAccessible(true);
            Map<?, ?> partialData = (Map<?, ?>) partialDataField.get(db);

            Field streamsField = BaseNautilusClient.class.getDeclaredField("streams");
            streamsField.setAccessible(true);
            Map<?, ?> streams = (Map<?, ?>) streamsField.get(db);

            System.out.println("count=" + seen.size());
            System.out.println("first=" + seen.get(0));
            System.out.println("tenth=" + seen.get(seen.size() - 1));
            System.out.println("follow=" + follow.stream().map(User::name).collect(Collectors.joining(",")));
            System.out.println("tail=" + tail.get(0).name());
            System.out.println("partialData=" + partialData.size());
            System.out.println("streams=" + streams.size());
        }
    }
}
