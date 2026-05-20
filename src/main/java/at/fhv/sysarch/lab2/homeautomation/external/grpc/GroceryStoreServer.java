package at.fhv.sysarch.lab2.homeautomation.external.grpc;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.grpc.javadsl.ServiceHandler;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.japi.function.Function;

import java.util.concurrent.CompletionStage;

public class GroceryStoreServer {

    public static void main(String[] args) {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "GroceryStoreServer");
        new GroceryStoreServer(system).run();
    }

    private final ActorSystem<Void> system;

    public GroceryStoreServer(ActorSystem<Void> system) {
        this.system = system;
    }

    public void run() {
        Function<HttpRequest, CompletionStage<HttpResponse>> service =
                GroceryStoreHandlerFactory.create(new GroceryStoreServiceImpl(), system);

        CompletionStage<ServerBinding> binding = Http.get(system)
                .newServerAt("127.0.0.1", 50052)
                .bind(service);

        binding.thenAccept(b ->
                System.out.println("Grocery Store Server bound to: " + b.localAddress())
        );
    }
}
