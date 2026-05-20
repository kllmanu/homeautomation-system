package at.fhv.sysarch.lab2.homeautomation.grocerystore;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Scheduler;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class GroceryStoreServiceImpl implements GroceryStore {

    private final ActorRef<MessageReceiver.IncomingOrder> messageReceiver;
    private final Scheduler scheduler;

    public GroceryStoreServiceImpl(ActorRef<MessageReceiver.IncomingOrder> messageReceiver, Scheduler scheduler) {
        this.messageReceiver = messageReceiver;
        this.scheduler = scheduler;
    }

    @Override
    public CompletionStage<OrderReceipt> processOrder(OrderRequest in) {
        System.out.println("GroceryStore (gRPC): Received order for " + in.getProductName());

        return AskPattern.ask(
                messageReceiver,
                replyTo -> new MessageReceiver.IncomingOrder(in, replyTo),
                Duration.ofSeconds(5),
                scheduler
        );
    }
}
