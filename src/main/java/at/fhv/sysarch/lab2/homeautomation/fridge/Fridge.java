package at.fhv.sysarch.lab2.homeautomation.fridge;

import at.fhv.sysarch.lab2.homeautomation.grocerystore.GroceryStore;
import at.fhv.sysarch.lab2.homeautomation.grocerystore.GroceryStoreClient;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;
import org.apache.pekko.grpc.GrpcClientSettings;

import java.util.*;

public class Fridge extends AbstractBehavior<FridgeModels.FridgeCommand> {

    public static final ServiceKey<FridgeModels.FridgeCommand> FRIDGE_SERVICE_KEY =
            ServiceKey.create(FridgeModels.FridgeCommand.class, "SmartFridge");

    private final int maxVolume;
    private final double maxWeight;
    
    private final Map<FridgeModels.Product, Integer> inventory = new HashMap<>();
    private final List<FridgeModels.Order> history = new ArrayList<>();
    private final List<ActorRef<FridgeModels.FridgeUpdate>> subscribers = new ArrayList<>();
    
    private final GroceryStoreClient groceryStoreClient;

    public static Behavior<FridgeModels.FridgeCommand> create(int maxVolume, double maxWeight) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(FRIDGE_SERVICE_KEY, context.getSelf()));
            return new Fridge(context, maxVolume, maxWeight);
        });
    }

    private Fridge(ActorContext<FridgeModels.FridgeCommand> context, int maxVolume, double maxWeight) {
        super(context);
        this.maxVolume = maxVolume;
        this.maxWeight = maxWeight;

        // Initialize gRPC client
        GrpcClientSettings settings = GrpcClientSettings.fromConfig("grocery-store", context.getSystem());
        this.groceryStoreClient = GroceryStoreClient.create(settings, context.getSystem());

        // Spawn internal sensors
        context.spawn(FridgeWeightSensor.create(context.getSelf()), "weightSensor");
        context.spawn(FridgeVolumeSensor.create(context.getSelf()), "volumeSensor");

        getContext().getLog().info("Smart Fridge started: MaxVolume={}, MaxWeight={}kg", maxVolume, maxWeight);
        
        // Initial stock
        addInitialStock();
    }

    private void addInitialStock() {
        FridgeModels.Product milk = new FridgeModels.Product("Milk", 1.2, 1.0);
        inventory.put(milk, 2);
    }

    @Override
    public Receive<FridgeModels.FridgeCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(FridgeModels.QueryProducts.class, this::onQueryProducts)
                .onMessage(FridgeModels.QueryOrderHistory.class, this::onQueryOrderHistory)
                .onMessage(FridgeModels.ConsumeProduct.class, this::onConsumeProduct)
                .onMessage(FridgeModels.OrderProduct.class, this::onOrderProduct)
                .onMessage(FridgeModels.Subscribe.class, this::onSubscribe)
                .onMessage(FridgeModels.OrderCompleted.class, this::onOrderCompleted)
                .onMessage(FridgeModels.OrderFailed.class, this::onOrderFailed)
                .onMessage(FridgeModels.QueryFridgeStatus.class, this::onQueryFridgeStatus)
                .onMessage(FridgeModels.ReadWeight.class, this::onReadWeight)
                .onMessage(FridgeModels.ReadVolume.class, this::onReadVolume)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<FridgeModels.FridgeCommand> onSubscribe(FridgeModels.Subscribe s) {
        subscribers.add(s.subscriber());
        return this;
    }

    private void notifySubscribers() {
        FridgeModels.FridgeUpdate update = new FridgeModels.FridgeUpdate();
        subscribers.forEach(s -> s.tell(update));
    }

    private Behavior<FridgeModels.FridgeCommand> onQueryProducts(FridgeModels.QueryProducts q) {
        List<FridgeModels.ProductInventory> list = new ArrayList<>();
        inventory.forEach((p, qnty) -> list.add(new FridgeModels.ProductInventory(p, qnty)));
        q.replyTo().tell(new FridgeModels.ProductsResponse(list));
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onQueryOrderHistory(FridgeModels.QueryOrderHistory q) {
        q.replyTo().tell(new FridgeModels.OrderHistoryResponse(new ArrayList<>(history)));
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onConsumeProduct(FridgeModels.ConsumeProduct c) {
        Optional<FridgeModels.Product> productOpt = inventory.keySet().stream()
                .filter(p -> p.name().equalsIgnoreCase(c.productName()))
                .findFirst();

        if (productOpt.isPresent()) {
            FridgeModels.Product p = productOpt.get();
            int currentCount = inventory.get(p);
            if (currentCount > 0) {
                inventory.put(p, currentCount - 1);
                getContext().getLog().info("Consumed: {}. Remaining: {}", p.name(), currentCount - 1);
                notifySubscribers();
                
                // Auto-reorder if out of stock
                if (currentCount - 1 == 0) {
                    getContext().getLog().info("Product {} out of stock! Triggering auto-reorder.", p.name());
                    getContext().getSelf().tell(new FridgeModels.OrderProduct(p, 2, null)); // Restock 2 items
                }
            } else {
                getContext().getLog().warn("Cannot consume {}: Out of stock", p.name());
            }
        } else {
            getContext().getLog().warn("Product {} not found in fridge", c.productName());
        }
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onOrderProduct(FridgeModels.OrderProduct o) {
        int currentVolume = inventory.values().stream().mapToInt(Integer::intValue).sum();
        double currentWeight = inventory.entrySet().stream()
                .mapToDouble(e -> e.getKey().weight() * e.getValue()).sum();

        int newVolume = currentVolume + o.quantity();
        double newWeight = currentWeight + (o.product().weight() * o.quantity());

        if (newVolume > maxVolume) {
            String msg = String.format("Order rejected: Insufficient volume (%d > %d)", newVolume, maxVolume);
            getContext().getLog().warn(msg);
            if (o.replyTo() != null) o.replyTo().tell(new FridgeModels.OrderResponse(false, msg));
            return this;
        }

        if (newWeight > maxWeight) {
            String msg = String.format("Order rejected: Insufficient weight capacity (%.1fkg > %.1fkg)", newWeight, maxWeight);
            getContext().getLog().warn(msg);
            if (o.replyTo() != null) o.replyTo().tell(new FridgeModels.OrderResponse(false, msg));
            return this;
        }

        // Spawn OrderProcessor
        getContext().spawn(OrderProcessor.create(o.product(), o.quantity(), getContext().getSelf(), groceryStoreClient), 
                "orderProcessor-" + UUID.randomUUID());

        if (o.replyTo() != null) o.replyTo().tell(new FridgeModels.OrderResponse(true, "Order placed successfully"));

        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onOrderCompleted(FridgeModels.OrderCompleted c) {
        if (c.receipt().getSuccessful()) {
            inventory.put(c.product(), inventory.getOrDefault(c.product(), 0) + c.receipt().getQuantity());
            history.add(new FridgeModels.Order(c.receipt().getOrderId(), c.product(), c.receipt().getQuantity(), c.receipt().getTotalCost(), true));
            getContext().getLog().info("Order completed and added to inventory: {} x {}", c.receipt().getQuantity(), c.product().name());
            notifySubscribers();
        } else {
            history.add(new FridgeModels.Order(c.receipt().getOrderId(), c.product(), c.receipt().getQuantity(), c.receipt().getTotalCost(), false));
            getContext().getLog().error("Order failed at grocery store: {}", c.receipt().getOrderId());
        }
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onOrderFailed(FridgeModels.OrderFailed f) {
        getContext().getLog().error("Order process failed: {}", f.reason());
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onQueryFridgeStatus(FridgeModels.QueryFridgeStatus q) {
        int currentVolume = inventory.values().stream().mapToInt(Integer::intValue).sum();
        double currentWeight = inventory.entrySet().stream()
                .mapToDouble(e -> e.getKey().weight() * e.getValue()).sum();
        q.replyTo().tell(new FridgeModels.FridgeStatusResponse(currentVolume, maxVolume, currentWeight, maxWeight));
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onReadWeight(FridgeModels.ReadWeight r) {
        double currentWeight = inventory.entrySet().stream()
                .mapToDouble(e -> e.getKey().weight() * e.getValue()).sum();
        r.replyTo().tell(new FridgeModels.WeightResponse(currentWeight));
        return this;
    }

    private Behavior<FridgeModels.FridgeCommand> onReadVolume(FridgeModels.ReadVolume r) {
        int currentVolume = inventory.values().stream().mapToInt(Integer::intValue).sum();
        r.replyTo().tell(new FridgeModels.VolumeResponse(currentVolume));
        return this;
    }

    private Fridge onPostStop() {
        getContext().getLog().info("Smart Fridge stopped");
        return this;
    }
}
