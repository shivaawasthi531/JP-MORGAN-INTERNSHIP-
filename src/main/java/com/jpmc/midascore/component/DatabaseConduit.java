package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class DatabaseConduit {
    static final Logger logger = LoggerFactory.getLogger(DatabaseConduit.class);
    private final UserRepository userRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    public DatabaseConduit(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) return;
        if (sender.getBalance() < transaction.getAmount()) return;

        // Call incentive API
        Incentive incentive = restTemplate.postForObject(
            "http://localhost:8080/incentive", transaction, Incentive.class);
        float bonus = (incentive != null) ? incentive.getAmount() : 0;

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + bonus);

        userRepository.save(sender);
        userRepository.save(recipient);

        if (sender.getName().equals("wilbur") || recipient.getName().equals("wilbur")) {
            UserRecord wilbur = sender.getName().equals("wilbur") ? sender : recipient;
            logger.info("WILBUR BALANCE: " + wilbur.getBalance());
        }
    }

    static class Incentive {
        private float amount;
        public float getAmount() { return amount; }
        public void setAmount(float amount) { this.amount = amount; }
    }
}