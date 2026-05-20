package at.fhv.sysarch.lab2.homeautomation.external.grpc;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.javadsl.CommandHandler;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;
import org.apache.pekko.persistence.typed.javadsl.Effect;

import java.util.ArrayList;
import java.util.List;

public class PersistenceActor extends EventSourcedBehavior<PersistenceActor.Command, PersistenceActor.Event, PersistenceActor.State> {

    public record PersistentReceipt(String orderId, String productName, int quantity, double totalCost, boolean successful) implements CborSerializable {}

    public interface Command extends CborSerializable {}
    public record PersistOrder(OrderReceipt receipt, ActorRef<OrderReceipt> replyTo) implements Command {}

    public interface Event extends CborSerializable {}
    public record OrderSaved(PersistentReceipt receipt) implements Event {}

    public static class State implements CborSerializable {
        private final List<PersistentReceipt> orders = new ArrayList<>();

        public void applyEvent(Event event) {
            if (event instanceof OrderSaved os) {
                orders.add(os.receipt());
            }
        }

        public List<PersistentReceipt> getOrders() {
            return orders;
        }
    }

    public static Behavior<Command> create() {
        return new PersistenceActor(PersistenceId.of("grocery-store", "unique-id"));
    }

    private PersistenceActor(PersistenceId persistenceId) {
        super(persistenceId);
    }

    @Override
    public State emptyState() {
        return new State();
    }

    @Override
    public CommandHandler<Command, Event, State> commandHandler() {
        return newCommandHandlerBuilder()
                .forAnyState()
                .onCommand(PersistOrder.class, this::onPersistOrder)
                .build();
    }

    private Effect<Event, State> onPersistOrder(State state, PersistOrder cmd) {
        PersistentReceipt pr = new PersistentReceipt(
                cmd.receipt().getOrderId(),
                cmd.receipt().getProductName(),
                cmd.receipt().getQuantity(),
                cmd.receipt().getTotalCost(),
                cmd.receipt().getSuccessful()
        );
        
        return Effect().persist(new OrderSaved(pr))
                .thenRun(newState -> cmd.replyTo().tell(cmd.receipt()));
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
        return newEventHandlerBuilder()
                .forAnyState()
                .onEvent(OrderSaved.class, (state, event) -> {
                    state.applyEvent(event);
                    return state;
                })
                .build();
    }
}
