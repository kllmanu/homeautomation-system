package at.fhv.sysarch.lab2.homeautomation.grocerystore;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class MessageReceiver extends AbstractBehavior<MessageReceiver.IncomingOrder> {

    public record IncomingOrder(OrderRequest request, ActorRef<OrderReceipt> replyTo) {}

    private final ActorRef<ProcessingActor.ProcessOrder> processingActor;

    public static Behavior<IncomingOrder> create(ActorRef<ProcessingActor.ProcessOrder> processingActor) {
        return Behaviors.setup(context -> new MessageReceiver(context, processingActor));
    }

    private MessageReceiver(ActorContext<IncomingOrder> context, ActorRef<ProcessingActor.ProcessOrder> processingActor) {
        super(context);
        this.processingActor = processingActor;
    }

    @Override
    public Receive<IncomingOrder> createReceive() {
        return newReceiveBuilder()
                .onMessage(IncomingOrder.class, this::onIncomingOrder)
                .build();
    }

    private Behavior<IncomingOrder> onIncomingOrder(IncomingOrder cmd) {
        getContext().getLog().info("MessageReceiver: Received order request for {}. Forwarding to processing.", cmd.request().getProductName());
        processingActor.tell(new ProcessingActor.ProcessOrder(cmd.request(), cmd.replyTo()));
        return this;
    }
}
