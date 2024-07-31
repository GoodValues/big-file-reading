package com.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop");
        SpringApplication.run(App.class, args);
    }
}
