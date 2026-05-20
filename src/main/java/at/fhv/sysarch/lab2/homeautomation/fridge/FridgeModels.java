package at.fhv.sysarch.lab2.homeautomation.fridge;

import at.fhv.sysarch.lab2.homeautomation.grocerystore.OrderReceipt;
import org.apache.pekko.actor.typed.ActorRef;
import java.util.List;

public class FridgeModels {
    public record Product(String name, double price, double weight) {}
    public record Order(String orderId, Product product, int quantity, double totalCost, boolean successful) {}

    public interface FridgeCommand {}

    // User/UI Commands
    public record QueryProducts(ActorRef<ProductsResponse> replyTo) implements FridgeCommand {}
    public record ProductsResponse(List<ProductInventory> products) {}
    public record ProductInventory(Product product, int quantity) {}

    public record QueryOrderHistory(ActorRef<OrderHistoryResponse> replyTo) implements FridgeCommand {}
    public record OrderHistoryResponse(List<Order> orders) {}

    public record ConsumeProduct(String productName) implements FridgeCommand {}
    public record OrderProduct(FridgeModels.Product product, int quantity) implements FridgeCommand {}
    public record Subscribe(ActorRef<FridgeUpdate> subscriber) implements FridgeCommand {}
    public record FridgeUpdate() {}

    // Internal Callback Commands
    public record OrderCompleted(OrderReceipt receipt, Product product) implements FridgeCommand {}
    public record OrderFailed(String reason) implements FridgeCommand {}

    // Sensor Commands
    public record QueryFridgeStatus(ActorRef<FridgeStatusResponse> replyTo) implements FridgeCommand {}
    public record FridgeStatusResponse(int currentVolume, int maxVolume, double currentWeight, double maxWeight) {}

    public record ReadWeight(ActorRef<WeightResponse> replyTo) implements FridgeCommand {}
    public record WeightResponse(double weight) {}

    public record ReadVolume(ActorRef<VolumeResponse> replyTo) implements FridgeCommand {}
    public record VolumeResponse(int volume) {}
}
