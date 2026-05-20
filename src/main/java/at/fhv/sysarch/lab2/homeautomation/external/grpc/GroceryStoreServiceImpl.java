package at.fhv.sysarch.lab2.homeautomation.external.grpc;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class GroceryStoreServiceImpl implements GroceryStore {

    @Override
    public CompletionStage<OrderReceipt> processOrder(OrderRequest in) {
        System.out.println("GroceryStore: Processing order for " + in.getQuantity() + "x " + in.getProductName());

        OrderReceipt receipt = OrderReceipt.newBuilder()
                .setOrderId(UUID.randomUUID().toString())
                .setProductName(in.getProductName())
                .setQuantity(in.getQuantity())
                .setTotalCost(in.getQuantity() * in.getPrice())
                .setSuccessful(true)
                .build();

        return CompletableFuture.completedFuture(receipt);
    }
}
