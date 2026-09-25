package com.hivemq;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.CountDownLatch;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MainDBJose {

    // Configuración PostgreSQL
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/prueba";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "Password";
    public static void main(String[] args) {
        final String host = "";
        final String username = "";
        final String password = "";
        final String topic = "zigbee2mqtt/0x385cfbfffec51fd9";
        CountDownLatch latch = new CountDownLatch(1);
        final Mqtt5AsyncClient client = MqttClient.builder()
                .useMqttVersion5()
                .serverHost(host)
                .serverPort(8883)
                .sslWithDefaultConfig()
                .buildAsync();
        client.connectWith()
                .simpleAuth()
                .username(username)
                .password(UTF_8.encode(password))
                .applySimpleAuth()
                .send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        System.err.println("Error MQTT: " + throwable.getMessage());
                        latch.countDown();
                    } else {
                        System.out.println("Conectado a HiveMQ Cloud.");
                        client.subscribeWith()
                                .topicFilter(topic)
                                .callback(publish -> {
// Leemos el payload como JSON
                                    String payload = UTF_8.decode(publish.getPayload().get()).toString();
                                    System.out.println("JSON recibido: " + payload);
                                    procesarJSON(payload);
                                })
                                .send();
                    }
                });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    private static void procesarJSON(String jsonString) {
        try {
// Parseamos el JSON
            JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();
// Extraemos la temperatura (ya viene en Celsius)
            double temperatura = jsonObject.get("temperature").getAsDouble();
            System.out.println("Temperatura: " + temperatura + "°C");
// Guardamos en la base de datos
            guardarTemperatura(temperatura);
        } catch (Exception e) {
            System.err.println("Error al procesar JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private static void guardarTemperatura(double temperatura) {
        String sql = "INSERT INTO dht11 (temp) VALUES (?)";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, temperatura);
            pstmt.executeUpdate();
            System.out.println("-> Guardado en dht11: " + temperatura + "°C");
        } catch (Exception e) {
            System.err.println("Error de base de datos: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
