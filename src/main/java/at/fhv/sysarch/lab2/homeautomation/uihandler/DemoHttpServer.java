package at.fhv.sysarch.lab2.homeautomation.uihandler;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.fridge.FridgeModels;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.marshalling.sse.EventStreamMarshalling;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.model.sse.ServerSentEvent;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.javadsl.Source;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class DemoHttpServer extends AllDirectives {

    private final ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment;
    private final ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment;
    private final ActorRef<AirCondition.AirConditionCommand> airCondition;
    private final ActorRef<MediaStation.MediaStationCommand> mediaStation;
    private final ActorRef<Blinds.BlindsCommand> blinds;
    private final ActorRef<FridgeModels.FridgeCommand> fridge;
    
    private final ActorRef<DashboardActor.DashboardCommand> dashboardActor;
    private final Source<ServerSentEvent, NotUsed> eventSource;
    
    private final String mode;
    private final Template indexTemplate;

    public DemoHttpServer(ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment,
                          ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment,
                          ActorRef<AirCondition.AirConditionCommand> airCondition,
                          ActorRef<MediaStation.MediaStationCommand> mediaStation,
                          ActorRef<Blinds.BlindsCommand> blinds,
                          ActorRef<FridgeModels.FridgeCommand> fridge,
                          ActorRef<DashboardActor.DashboardCommand> dashboardActor,
                          Source<ServerSentEvent, NotUsed> eventSource,
                          String mode) {
        this.temperatureEnvironment = temperatureEnvironment;
        this.weatherEnvironment = weatherEnvironment;
        this.airCondition = airCondition;
        this.mediaStation = mediaStation;
        this.blinds = blinds;
        this.fridge = fridge;
        this.dashboardActor = dashboardActor;
        this.eventSource = eventSource;
        this.mode = mode;

        TemplateLoader loader = new ClassPathTemplateLoader("/templates", ".hbs");
        Handlebars handlebars = new Handlebars(loader);
        try {
            this.indexTemplate = handlebars.compile("index");
        } catch (IOException e) {
            throw new RuntimeException("Could not compile handlebars template", e);
        }
    }

    public Route createRoute(ActorSystem<Void> system) {
        return concat(
                pathSingleSlash(() ->
                        get(() -> {
                            CompletionStage<DashboardActor.DashboardState> dashboardStage = AskPattern.ask(
                                    dashboardActor, DashboardActor.GetDashboardState::new, Duration.ofSeconds(2), system.scheduler());
                            
                            CompletionStage<FridgeModels.ProductsResponse> fridgeProductsStage = AskPattern.ask(
                                    fridge, FridgeModels.QueryProducts::new, Duration.ofSeconds(2), system.scheduler());
                            
                            CompletionStage<FridgeModels.OrderHistoryResponse> fridgeHistoryStage = AskPattern.ask(
                                    fridge, FridgeModels.QueryOrderHistory::new, Duration.ofSeconds(2), system.scheduler());
                            
                            CompletionStage<FridgeModels.FridgeStatusResponse> fridgeStatusStage = AskPattern.ask(
                                    fridge, FridgeModels.QueryFridgeStatus::new, Duration.ofSeconds(2), system.scheduler());

                            return onSuccess(dashboardStage
                                            .thenCombine(fridgeProductsStage, Pair::new)
                                            .thenCombine(fridgeHistoryStage, Pair::new)
                                            .thenCombine(fridgeStatusStage, Pair::new),
                                    combined -> {
                                        var p_history = combined.first(); 
                                        var p_products = p_history.first(); 
                                        
                                        DashboardActor.DashboardState state = p_products.first();
                                        FridgeModels.ProductsResponse products = p_products.second();
                                        FridgeModels.OrderHistoryResponse history = p_history.second();
                                        FridgeModels.FridgeStatusResponse status = combined.second();

                                        Map<String, Object> model = new HashMap<>();
                                        model.put("title", "Home Automation Environment Control");
                                        model.put("mode", this.mode);
                                        model.put("isManual", this.mode.equals("MANUAL"));

                                        model.put("acState", state.acOn() ? "ON" : "OFF");
                                        model.put("acClass", state.acOn() ? "status-on" : "status-off");
                                        model.put("blindsState", state.blindsClosed() ? "CLOSED" : "OPEN");
                                        model.put("blindsClass", state.blindsClosed() ? "status-closed" : "status-open");
                                        model.put("mediaState", state.mediaPlaying() ? "PLAYING" : "IDLE");
                                        model.put("mediaClass", state.mediaPlaying() ? "status-playing" : "status-idle");
                                        
                                        model.put("temperature", String.format("%.1f", state.temperature()));
                                        model.put("tempEmoji", state.temperature() < 10 ? "❄️" : (state.temperature() < 20 ? "🌡️" : (state.temperature() < 30 ? "☀️" : "🔥")));
                                        
                                        String wString = state.weather();
                                        model.put("weather", wString);
                                        model.put("weatherEmoji", wString.contains("SUNNY") ? "☀️" : (wString.contains("CLOUDY") ? "☁️" : (wString.contains("RAINY") ? "🌧️" : "🛸")));
                                        
                                        model.put("fridgeProducts", products.products());
                                        model.put("fridgeHistory", history.orders());
                                        model.put("fridgeCurrentWeight", String.format("%.1f", status.currentWeight()));
                                        model.put("fridgeMaxWeight", String.format("%.1f", status.maxWeight()));
                                        model.put("fridgeCurrentVolume", status.currentVolume());
                                        model.put("fridgeMaxVolume", status.maxVolume());
                                        
                                        try {
                                            String rendered = indexTemplate.apply(model);
                                            return complete(HttpEntities.create(ContentTypes.TEXT_HTML_UTF8, rendered));
                                        } catch (IOException e) {
                                            return complete("Error rendering template: " + e.getMessage());
                                        }
                                    }
                            );
                        })
                ),
                path("events", () ->
                        get(() -> completeOK(eventSource, EventStreamMarshalling.toEventStream()))
                ),
                pathPrefix("fridge", () -> concat(
                        path("consume", () -> post(() -> formField("productName", name -> {
                            this.fridge.tell(new FridgeModels.ConsumeProduct(name));
                            return complete("Consumed " + name);
                        }))),
                        path("order", () -> post(() -> formField("productName", name -> formField("quantity", q -> {
                            FridgeModels.Product p;
                            switch (name.toLowerCase()) {
                                case "eggs": p = new FridgeModels.Product("Eggs", 2.99, 0.6); break;
                                case "milk": p = new FridgeModels.Product("Milk", 1.20, 1.0); break;
                                case "beer": p = new FridgeModels.Product("Beer", 0.89, 0.5); break;
                                default: p = new FridgeModels.Product(name, 1.0, 1.0);
                            }

                            CompletionStage<FridgeModels.OrderResponse> reply = AskPattern.ask(
                                    fridge, replyTo -> new FridgeModels.OrderProduct(p, Integer.parseInt(q), replyTo),
                                    Duration.ofSeconds(2), system.scheduler()
                            );

                            return onSuccess(reply, response -> {
                                if (response.success()) {
                                    return complete(response.message());
                                } else {
                                    return complete(org.apache.pekko.http.javadsl.model.StatusCodes.BAD_REQUEST, response.message());
                                }
                            });
                        }))))
                )),
                path("temperature", () -> post(() -> {
                            if (!this.mode.equals("MANUAL")) return complete("Manual override is disabled");
                            return formField("value", value -> {
                                this.temperatureEnvironment.tell(new TemperatureEnvironment.SetTemperature(Double.parseDouble(value)));
                                return complete("Temperature set");
                            });
                        })
                ),
                path("weather", () -> post(() -> {
                            if (!this.mode.equals("MANUAL")) return complete("Manual override is disabled");
                            return formField("value", value -> {
                                this.weatherEnvironment.tell(new WeatherEnvironment.SetWeather(WeatherEnvironment.Weather.valueOf(value.toUpperCase())));
                                return complete("Weather set");
                            });
                        })
                ),
                path("aircondition", () -> post(() -> formField("value", value -> {
                            this.airCondition.tell(new AirCondition.PowerAirCondition(Boolean.parseBoolean(value)));
                            return complete("AirCondition toggled");
                        }))
                ),
                path("mediastation-play", () -> post(() -> {
                            CompletionStage<MediaStation.PlayMovieResponse> reply = AskPattern.ask(
                                    mediaStation, MediaStation.PlayMovie::new, Duration.ofSeconds(2), system.scheduler());
                            return onSuccess(reply, response -> {
                                if (response.success()) return complete(response.message());
                                else return complete(org.apache.pekko.http.javadsl.model.StatusCodes.BAD_REQUEST, response.message());
                            });
                        })
                ),
                path("mediastation-stop", () -> post(() -> {
                            this.mediaStation.tell(new MediaStation.StopMovie());
                            return complete("MediaStation stopped");
                        })
                ),
                path("blinds", () -> post(() -> formField("value", value -> {
                            this.blinds.tell(new Blinds.ControlBlinds(Boolean.parseBoolean(value)));
                            return complete("Blinds toggled");
                        }))
                )
        );
    }
}