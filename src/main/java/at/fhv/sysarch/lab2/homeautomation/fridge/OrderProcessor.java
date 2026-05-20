package at.fhv.sysarch.lab2.homeautomation.fridge;

import at.fhv.sysarch.lab2.homeautomation.external.grpc.GroceryStore;
import at.fhv.sysarch.lab2.homeautomation.external.grpc.OrderRequest;
import at.fhv.sysarch.lab2.homeautomation.external.grpc.OrderReceipt;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.concurrent.CompletionStage;

public class OrderProcessor extends AbstractBehavior<OrderProcessor.Command> {

    public interface Command {}
    private record InternalReceipt(OrderReceipt receipt) implements Command {}
    private record InternalFailure(Throwable cause) implements Command {}

    private final FridgeModels.Product product;
    private final int quantity;
    private final ActorRef<FridgeModels.FridgeCommand> replyTo;
    private final GroceryStore client;

    public static Behavior<Command> create(FridgeModels.Product product, int quantity, ActorRef<FridgeModels.FridgeCommand> replyTo, GroceryStore client) {
        return Behaviors.setup(context -> new OrderProcessor(context, product, quantity, replyTo, client));
    }

    private OrderProcessor(ActorContext<Command> context, FridgeModels.Product product, int quantity, ActorRef<FridgeModels.FridgeCommand> replyTo, GroceryStore client) {
        super(context);
        this.product = product;
        this.quantity = quantity;
        this.replyTo = replyTo;
        this.client = client;

        OrderRequest request = OrderRequest.newBuilder()
                .setProductName(product.name())
                .setPrice(product.price())
                .setWeight(product.weight())
                .setQuantity(quantity)
                .build();

        getContext().getLog().info("OrderProcessor: Processing order for {} x {}", quantity, product.name());
        
        CompletionStage<OrderReceipt> response = client.processOrder(request);
        
        context.pipeToSelf(response, (res, exc) -> {
            if (res != null) return new InternalReceipt(res);
            else return new InternalFailure(exc);
        });
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(InternalReceipt.class, this::onReceipt)
                .onMessage(InternalFailure.class, this::onFailure)
                .build();
    }

    private Behavior<Command> onReceipt(InternalReceipt r) {
        getContext().getLog().info("OrderProcessor: Order {} successful: {}", r.receipt().getOrderId(), r.receipt().getSuccessful());
        replyTo.tell(new FridgeModels.OrderCompleted(r.receipt(), product));
        return Behaviors.stopped();
    }

    private Behavior<Command> onFailure(InternalFailure f) {
        getContext().getLog().error("OrderProcessor: gRPC call failed", f.cause());
        replyTo.tell(new FridgeModels.OrderFailed(f.cause().getMessage()));
        return Behaviors.stopped();
    }
}
