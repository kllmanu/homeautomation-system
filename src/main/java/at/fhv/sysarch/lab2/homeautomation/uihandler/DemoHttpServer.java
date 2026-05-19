package at.fhv.sysarch.lab2.homeautomation.uihandler;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DemoHttpServer extends AllDirectives {

    private final ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment;
    private final ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment;
    private final ActorRef<AirCondition.AirConditionCommand> airCondition;
    private final ActorRef<MediaStation.MediaStationCommand> mediaStation;
    private final ActorRef<Blinds.BlindsCommand> blinds;
    private final String mode;
    private final Template indexTemplate;

    public DemoHttpServer(ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> temperatureEnvironment,
                          ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnvironment,
                          ActorRef<AirCondition.AirConditionCommand> airCondition,
                          ActorRef<MediaStation.MediaStationCommand> mediaStation,
                          ActorRef<Blinds.BlindsCommand> blinds,
                          String mode) {
        this.temperatureEnvironment = temperatureEnvironment;
        this.weatherEnvironment = weatherEnvironment;
        this.airCondition = airCondition;
        this.mediaStation = mediaStation;
        this.blinds = blinds;
        this.mode = mode;

        TemplateLoader loader = new ClassPathTemplateLoader("/templates", ".hbs");
        Handlebars handlebars = new Handlebars(loader);
        try {
            this.indexTemplate = handlebars.compile("index");
        } catch (IOException e) {
            throw new RuntimeException("Could not compile handlebars template", e);
        }
    }

    public Route createRoute() {
        return concat(
                pathSingleSlash(() ->
                        get(() -> {
                            Map<String, Object> model = new HashMap<>();
                            model.put("title", "Home Automation Environment Control");
                            model.put("mode", this.mode);
                            model.put("isManual", this.mode.equals("MANUAL"));
                            try {
                                String rendered = indexTemplate.apply(model);
                                return complete(HttpEntities.create(ContentTypes.TEXT_HTML_UTF8, rendered));
                            } catch (IOException e) {
                                return complete("Error rendering template: " + e.getMessage());
                            }
                        })
                ),
                path("hello", () -> get(() -> complete("<h1>Say hello to pekko-http</h1>"))),
                path("temperature", () ->
                        post(() -> {
                            if (!this.mode.equals("MANUAL")) {
                                return complete("Manual override is disabled in " + this.mode + " mode");
                            }
                            return formField("value", value -> {
                                double temp = Double.parseDouble(value);
                                this.temperatureEnvironment.tell(new TemperatureEnvironment.SetTemperature(temp));
                                return complete("Temperature set to " + temp);
                            });
                        })
                ),
                path("weather", () ->
                        post(() -> {
                            if (!this.mode.equals("MANUAL")) {
                                return complete("Manual override is disabled in " + this.mode + " mode");
                            }
                            return formField("value", value -> {
                                WeatherEnvironment.Weather weather = WeatherEnvironment.Weather.valueOf(value.toUpperCase());
                                this.weatherEnvironment.tell(new WeatherEnvironment.SetWeather(weather));
                                return complete("Weather set to " + weather);
                            });
                        })
                ),
                path("aircondition", () ->
                        post(() -> formField("value", value -> {
                            boolean power = Boolean.parseBoolean(value);
                            this.airCondition.tell(new AirCondition.PowerAirCondition(power));
                            return complete("AirCondition power set to " + power);
                        }))
                ),
                path("mediastation-play", () ->
                        post(() -> {
                            System.out.println("Server: Received MediaStation Play request");
                            this.mediaStation.tell(new MediaStation.PlayMovie());
                            return complete("MediaStation: Play movie");
                        })
                ),
                path("mediastation-stop", () ->
                        post(() -> {
                            System.out.println("Server: Received MediaStation Stop request");
                            this.mediaStation.tell(new MediaStation.StopMovie());
                            return complete("MediaStation: Stop movie");
                        })
                ),
                path("blinds", () ->
                        post(() -> formField("value", value -> {
                            boolean close = Boolean.parseBoolean(value);
                            this.blinds.tell(new Blinds.ControlBlinds(close));
                            return complete("Blinds " + (close ? "closed" : "opened"));
                        }))
                )
        );
    }
}
