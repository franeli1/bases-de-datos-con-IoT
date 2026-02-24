package com.hivemq;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MainDB {

    //Configuracion PosgreSQL
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/prueba";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "oretania";

    public static void main(String[] args) {
        final String host = "bdead0b1712a4c288dea50631d02be89.s1.eu.hivemq.cloud";
        final String username = "Francisco";
        final String password = "Franeli372626";
        final String topic = "temp";

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
                .whenComplete(((mqtt5ConnAck, throwable) -> {
                    if (throwable != null) {
                        System.err.println("Error MQTT: " + throwable.getMessage());
                        latch.countDown();
                    } else {
                        System.out.println("Conectando a hiveMQ Cloud.");

                        client.subscribeWith()
                                .topicFilter(topic)
                                .callback(mqtt5Publish -> {
                                    //Leemos el payload directamente como un número
                                    String payload = UTF_8.decode(mqtt5Publish.getPayload().get()).toString();
                                    System.out.println("Valor recibido: " + payload);
                                    guardarTemperatura(payload);
                                })
                                .send();
                    }
                }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private static void guardarTemperatura(String valorRaw) {
        try {
            //convertimos el texto recibido directamente a double
            double temperatura = Double.parseDouble(valorRaw.trim());

            String sql = "INSERT INTO dht11 (temp) VALUES(?)";

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setDouble(1, temperatura);
                pstmt.executeUpdate();
                System.out.println("-> Guardado en dht11: " + temperatura);

            }
        } catch (NumberFormatException e) {
            System.err.println("El valor recibido no es un número válido: " + valorRaw);
        } catch (Exception e) {
            System.err.println("Error de base de datos: " + e.getMessage());
        }
    }
}