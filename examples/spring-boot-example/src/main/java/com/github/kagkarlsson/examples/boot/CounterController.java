package com.github.kagkarlsson.examples.boot;

import java.util.HashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/counter")
public class CounterController {
    private final CounterService counter;

    public CounterController(CounterService counter) {
        this.counter = counter;
    }

    @GetMapping
    public Map<String, Long> displayCounter() {
        Map<String, Long> data = new HashMap<>();
        data.put("recurring_executions", counter.read());

        return data;
    }
}
