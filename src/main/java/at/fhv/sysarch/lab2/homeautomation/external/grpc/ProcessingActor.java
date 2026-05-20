package at.fhv.sysarch.lab2.homeautomation.external.grpc;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.UUID;

public class ProcessingActor extends AbstractBehavior<ProcessingActor.ProcessOrder> {

    public record ProcessOrder(OrderRequest request, ActorRef<OrderReceipt> replyTo) {}

    private final ActorRef<PersistenceActor.Command> persistenceActor;

    public static Behavior<ProcessOrder> create(ActorRef<PersistenceActor.Command> persistenceActor) {
        return Behaviors.setup(context -> new ProcessingActor(context, persistenceActor));
    }

    private ProcessingActor(ActorContext<ProcessOrder> context, ActorRef<PersistenceActor.Command> persistenceActor) {
        super(context);
        this.persistenceActor = persistenceActor;
    }

    @Override
    public Receive<ProcessOrder> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessOrder.class, this::onProcessOrder)
                .build();
    }

    private Behavior<ProcessOrder> onProcessOrder(ProcessOrder cmd) {
        OrderRequest req = cmd.request();
        
        // Validation rules
        if (req.getQuantity() <= 0) {
            getContext().getLog().warn("Validation failed: Quantity must be positive");
            cmd.replyTo().tell(OrderReceipt.newBuilder()
                    .setOrderId("N/A")
                    .setProductName(req.getProductName())
                    .setQuantity(req.getQuantity())
                    .setTotalCost(0)
                    .setSuccessful(false)
                    .build());
            return this;
        }

        if (req.getPrice() < 0 || req.getWeight() < 0) {
             getContext().getLog().warn("Validation failed: Negative price or weight");
             cmd.replyTo().tell(OrderReceipt.newBuilder()
                    .setOrderId("N/A")
                    .setProductName(req.getProductName())
                    .setQuantity(req.getQuantity())
                    .setTotalCost(0)
                    .setSuccessful(false)
                    .build());
            return this;
        }

        // Generate ID and receipt
        String orderId = UUID.randomUUID().toString();
        OrderReceipt receipt = OrderReceipt.newBuilder()
                .setOrderId(orderId)
                .setProductName(req.getProductName())
                .setQuantity(req.getQuantity())
                .setTotalCost(req.getQuantity() * req.getPrice())
                .setSuccessful(true)
                .build();

        getContext().getLog().info("Validation successful for order {}. Forwarding to persistence.", orderId);
        
        persistenceActor.tell(new PersistenceActor.PersistOrder(receipt, cmd.replyTo()));
        
        return this;
    }
}
