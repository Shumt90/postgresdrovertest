package org.example;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.api.PostgresqlRow;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 */
public class App {

    static String query = "select id from matchevent order by id";

    public static void main(String[] args) throws InterruptedException, SQLException {


        var aLong = new AtomicLong();

        queryR2dbc(aLong)
                .flatMap(r->r.map((a,b)->a.get(0)).map(v->{
                    System.out.println("get by R2dbc "+v);
                    return "";
                }))
                .blockFirst();
        System.out.println("time first element by R2dbc: " + (System.currentTimeMillis() - aLong.get()));

        queryJdbc(aLong);
        System.out.println("time by Jdbc: " + (System.currentTimeMillis() - aLong.get()));

    }

    private static void queryJdbc(AtomicLong aLong) throws SQLException {

        var connection = DriverManager
                .getConnection("jdbc:postgresql://dev-db.sport24.local/8news_site", "sosi", "sosi");

        aLong.set(System.currentTimeMillis());
        System.out.println("connection created, begin query");

        var rs = connection.createStatement().executeQuery(query);
        rs.next();

        System.out.println("get by jdbc "+rs.getString("id"));
    }

    static Flux<PostgresqlResult> queryR2dbc(AtomicLong aLong) {

        ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:postgresql://sosi:sosi@dev-db.sport24.local/8news_site");
        PostgresqlConnection conn = (PostgresqlConnection) Mono.from(connectionFactory.create()).block();

        aLong.set(System.currentTimeMillis());
        System.out.println("connection created, begin query");

        return conn.createStatement(query).execute();
    }
}
