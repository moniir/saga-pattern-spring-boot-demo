package com.example.mhb.payments.service;

import com.example.mhb.core.dto.Payment;

import java.util.List;

public interface PaymentService {
    List<Payment> findAll();

    Payment process(Payment payment);
}
