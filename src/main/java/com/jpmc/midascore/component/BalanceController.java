package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.web.bind.annotation.*;

@RestController
public class BalanceController {
    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam long userId) {
        UserRecord user = userRepository.findById(userId);
        if (user == null) return new Balance(0);
        return new Balance(user.getBalance());
    }

    static class Balance {
        private float amount;
        public Balance(float amount) { this.amount = amount; }
        public float getAmount() { return amount; }
        public void setAmount(float amount) { this.amount = amount; }
    }
}