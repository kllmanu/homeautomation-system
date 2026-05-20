package at.fhv.sysarch.lab2.homeautomation.external.grpc;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.japi.function.Function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.CompletionStage;

public class GroceryStoreServer {

    public interface Command {}
    private record StartServer(ActorRef<MessageReceiver.IncomingOrder> receiver) implements Command {}

    public static void main(String[] args) {
        initDatabase();

        Behavior<Command> mainBehavior = Behaviors.setup(context -> {
            ActorRef<PersistenceActor.Command> persistence = context.spawn(PersistenceActor.create(), "persistenceActor");
            ActorRef<ProcessingActor.ProcessOrder> processing = context.spawn(ProcessingActor.create(persistence), "processingActor");
            ActorRef<MessageReceiver.IncomingOrder> receiver = context.spawn(MessageReceiver.create(processing), "messageReceiver");

            new GroceryStoreServer(context.getSystem()).run(receiver);
            return Behaviors.empty();
        });

        ActorSystem.create(mainBehavior, "GroceryStoreServer");
    }

    private static void initDatabase() {
        String url = "jdbc:h2:mem:grocery-store;DB_CLOSE_DELAY=-1;USER=sa;PASSWORD=;DATABASE_TO_UPPER=FALSE";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            System.out.println("Initializing H2 database schema (url=" + url + ")...");

            // Pekko Persistence JDBC Schema for H2 (Fully quoted for case-sensitivity)
            stmt.execute("CREATE TABLE IF NOT EXISTS \"event_journal\" (" +
                    "\"ordering\" BIGINT AUTO_INCREMENT, " +
                    "\"persistence_id\" VARCHAR(255) NOT NULL, " +
                    "\"sequence_number\" BIGINT NOT NULL, " +
                    "\"deleted\" BOOLEAN DEFAULT FALSE NOT NULL, " +
                    "\"writer\" VARCHAR(255) NOT NULL, " +
                    "\"write_timestamp\" BIGINT NOT NULL, " +
                    "\"adapter_manifest\" VARCHAR(255) NOT NULL, " +
                    "\"event_ser_id\" INTEGER NOT NULL, " +
                    "\"event_ser_manifest\" VARCHAR(255) NOT NULL, " +
                    "\"event_payload\" BYTEA NOT NULL, " +
                    "\"meta_ser_id\" INTEGER, " +
                    "\"meta_ser_manifest\" VARCHAR(255), " +
                    "\"meta_payload\" BYTEA, " +
                    "PRIMARY KEY(\"persistence_id\", \"sequence_number\"))");

            stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS \"event_journal_ordering_idx\" ON \"event_journal\"(\"ordering\")");

            stmt.execute("CREATE TABLE IF NOT EXISTS \"event_tag\" (" +
                    "\"event_id\" BIGINT NOT NULL, " +
                    "\"tag\" VARCHAR(255) NOT NULL, " +
                    "PRIMARY KEY(\"event_id\", \"tag\"))");

            stmt.execute("CREATE TABLE IF NOT EXISTS \"snapshot\" (" +
                    "\"persistence_id\" VARCHAR(255) NOT NULL, " +
                    "\"sequence_number\" BIGINT NOT NULL, " +
                    "\"created\" BIGINT NOT NULL, " +
                    "\"snapshot_ser_id\" INTEGER NOT NULL, " +
                    "\"snapshot_ser_manifest\" VARCHAR(255) NOT NULL, " +
                    "\"snapshot_payload\" BYTEA NOT NULL, " +
                    "\"meta_ser_id\" INTEGER, " +
                    "\"meta_ser_manifest\" VARCHAR(255), " +
                    "\"meta_payload\" BYTEA, " +
                    "PRIMARY KEY(\"persistence_id\", \"sequence_number\"))");

            System.out.println("H2 schema initialized.");

        } catch (Exception e) {
            System.err.println("Failed to initialize database: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private final ActorSystem<?> system;

    public GroceryStoreServer(ActorSystem<?> system) {
        this.system = system;
    }

    public void run(ActorRef<MessageReceiver.IncomingOrder> receiver) {
        Function<HttpRequest, CompletionStage<HttpResponse>> service =
                GroceryStoreHandlerFactory.create(new GroceryStoreServiceImpl(receiver, system.scheduler()), system);

        CompletionStage<ServerBinding> binding = Http.get(system)
                .newServerAt("127.0.0.1", 50052)
                .bind(service);

        binding.thenAccept(b ->
                System.out.println("Grocery Store Server (Actor Decoupled) bound to: " + b.localAddress())
        );
    }
}
