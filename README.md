# Saga Pattern Spring Boot Demo

A comprehensive demonstration of the **Saga Orchestration Pattern** using Spring Boot, Apache Kafka, and microservices architecture. This project showcases how to handle distributed transactions across multiple services with eventual consistency.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [API Endpoints](#api-endpoints)
- [Saga Flow](#saga-flow)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Testing](#testing)
- [Docker Compose Options](#docker-compose-options)

## 🎯 Overview

This project demonstrates the **Saga Orchestration Pattern** for managing distributed transactions across microservices. When a customer places an order, the system must coordinate multiple services (products, payments, orders) to complete the transaction. If any step fails, compensating transactions are triggered to maintain data consistency.

## 🏗️ Architecture

The system uses an event-driven architecture with Apache Kafka as the message broker. The Orders Service acts as the saga orchestrator, coordinating the workflow across services.

```
┌─────────────┐
│   Orders    │◄─── POST /orders (Customer places order)
│   Service   │
│  (Port 8080)│
└──────┬──────┘
       │ Orchestrates Saga
       ├──► 1. Reserve Product
       │    ┌─────────────┐
       │    │  Products   │
       │    │   Service   │
       │    │ (Port 8081) │
       │    └─────────────┘
       │
       ├──► 2. Process Payment
       │    ┌─────────────┐         ┌──────────────────┐
       │    │  Payments   │────────►│  Credit Card     │
       │    │   Service   │         │ Processor Service│
       │    │ (Port 8082) │         │   (Port 8084)    │
       │    └─────────────┘         └──────────────────┘
       │
       └──► 3. Approve Order
            (If all steps succeed)

       All services communicate via Kafka topics
       ┌─────────────────────────────┐
       │   Apache Kafka Cluster      │
       │   (3-node KRaft cluster)    │
       │   Ports: 9092, 9094, 9096   │
       │                             │
       │   + Kafka UI (Port 8088)    │
       └─────────────────────────────┘
```

## 🔧 Services

### 1. **Orders Service** (Port 8080)
- **Role**: Saga Orchestrator
- **Responsibilities**:
  - Receives order creation requests
  - Coordinates the entire saga workflow
  - Maintains order history and status
  - Handles compensating transactions on failures

### 2. **Products Service** (Port 8081)
- **Responsibilities**:
  - Manages product inventory
  - Reserves products for orders
  - Handles reservation failures

### 3. **Payments Service** (Port 8082)
- **Responsibilities**:
  - Processes payment transactions
  - Communicates with credit card processor
  - Handles payment failures

### 4. **Credit Card Processor Service** (Port 8084)
- **Responsibilities**:
  - Simulates external payment gateway
  - Validates and processes credit card transactions

### 5. **Core Module**
- Shared DTOs, events, commands, and types
- Used by all services for consistent messaging

## 🛠️ Technology Stack

- **Java**: 17
- **Spring Boot**: 3.2.5
- **Apache Kafka**: Latest (Confluent Platform - KRaft mode, 3-node cluster)
- **Database**: H2 (In-memory) - Separate database per service
- **Message Serialization**: JSON
- **Build Tool**: Maven
- **Containerization**: Docker & Docker Compose
- **Kafka UI**: Provectus Kafka UI (for monitoring)

## ✅ Prerequisites

- **Java 17** or higher
- **Maven 3.6+**
- **Docker** and **Docker Compose**
- At least **4GB RAM** available for Docker containers

## 🚀 Getting Started

### 1. Start Kafka Cluster

```bash
docker-compose -f docker-compose-kafka-cluster-confluent.yml up -d
```

This will start:
- A 3-node Kafka cluster (Confluent Platform) in KRaft mode on ports **9092**, **9094**, and **9096**
- **Kafka UI** on port **8088** for monitoring topics, consumers, and messages

Access Kafka UI at: http://localhost:8088

### 2. Build the Project

```bash
mvn clean install
```

### 3. Start All Services

You can start each service individually:

```bash
# Terminal 1 - Orders Service
cd orders-service
mvn spring-boot:run

# Terminal 2 - Products Service
cd products-service
mvn spring-boot:run

# Terminal 3 - Payments Service
cd payments-service
mvn spring-boot:run

# Terminal 4 - Credit Card Processor Service
cd credit-card-processor-service
mvn spring-boot:run
```

Or run the built JARs:

```bash
java -jar orders-service/target/orders-service-1.0.0.jar
java -jar products-service/target/product-service-1.0.0.jar
java -jar payments-service/target/payments-service-1.0.0.jar
java -jar credit-card-processor-service/target/credit-card-processor-service-1.0.0.jar
```

## 📡 API Endpoints

### Orders Service (http://localhost:8080)

#### Create Order
```http
POST /orders
Content-Type: application/json

{
  "customerId": "550e8400-e29b-41d4-a716-446655440000",
  "productId": "650e8400-e29b-41d4-a716-446655440001",
  "productQuantity": 2
}
```

**Response (202 Accepted)**:
```json
{
  "orderId": "750e8400-e29b-41d4-a716-446655440002",
  "customerId": "550e8400-e29b-41d4-a716-446655440000",
  "productId": "650e8400-e29b-41d4-a716-446655440001",
  "productQuantity": 2,
  "orderStatus": "CREATED"
}
```

#### Get Order History
```http
GET /orders/{orderId}/history
```

**Response (200 OK)**:
```json
[
  {
    "orderId": "750e8400-e29b-41d4-a716-446655440002",
    "orderStatus": "CREATED",
    "timestamp": "2026-05-18T10:30:00"
  },
  {
    "orderId": "750e8400-e29b-41d4-a716-446655440002",
    "orderStatus": "APPROVED",
    "timestamp": "2026-05-18T10:30:05"
  }
]
```

### Products Service (http://localhost:8081)

#### Get All Products
```http
GET /products
```

#### Create Product
```http
POST /products
Content-Type: application/json

{
  "productName": "Sample Product",
  "productPrice": 99.99,
  "productQuantity": 100
}
```

### Credit Card Processor Service (http://localhost:8084)

#### Process Payment
```http
POST /ccps/process
Content-Type: application/json

{
  "cardNumber": "4532123456789012",
  "amount": 199.98
}
```

## 🔄 Saga Flow

### Success Flow

1. **Order Created Event** → Orders Service publishes to Kafka
2. **Reserve Product Command** → Products Service receives and reserves inventory
3. **Product Reserved Event** → Orders Service receives confirmation
4. **Process Payment Command** → Payments Service initiates payment
5.  **Payment Processed Event** → Orders Service receives confirmation
6. **Approve Order Command** → Orders Service finalizes the order
7. **Order Approved Event** → Order status updated to APPROVED

### Failure Flow (with Compensation)

If **Product Reservation Fails**:
- Products Service publishes `ProductReservationFailedEvent`
- Orders Service marks order as **REJECTED**
- No payment is attempted

If **Payment Fails**:
- Payments Service publishes `PaymentFailedEvent`
- Orders Service marks order as **REJECTED**
- (Product reservation rollback can be implemented)

## 📁 Project Structure

```
saga-pattern-spring-boot-demo/
├── core/                              # Shared module
│   └── src/main/java/com/example/mhb/core/
│       ├── dto/                       # Data Transfer Objects
│       │   ├── commands/             # Command objects
│       │   └── events/               # Event objects
│       └── types/                    # Enums and types
├── orders-service/                    # Saga Orchestrator
│   └── src/main/java/com/example/mhb/orders/
│       ├── saga/                     # OrderSaga orchestration logic
│       ├── service/                  # Business logic
│       └── web/controller/           # REST endpoints
├── products-service/                  # Product inventory management
│   └── src/main/java/com/example/mhb/products/
├── payments-service/                  # Payment processing
│   └── src/main/java/com/example/mhb/payments/
├── credit-card-processor-service/     # External payment gateway simulation
│   └── src/main/java/com/example/mhb/ccps/
├── docker-compose-kafka-cluster-confluent.yml  # Confluent Kafka setup
├── docker-compose.yml                          # Alternative Bitnami Kafka setup
└── pom.xml                                     # Parent POM
```

## ⚙️ Configuration

### Kafka Topics

The services communicate through the following Kafka topics:
- `orders-events-topic` - Order lifecycle events
- `orders-commands-topic` - Order commands
- `products-events-topic` - Product events
- `products-commands-topic` - Product commands
- `payments-events-topic` - Payment events
- `payments-commands-topic` - Payment commands

### Service Ports

| Service                    | Port | H2 Console                        |
|----------------------------|------|-----------------------------------|
| Orders Service             | 8080 | http://localhost:8080/h2-console |
| Products Service           | 8081 | http://localhost:8081/h2-console |
| Payments Service           | 8082 | http://localhost:8082/h2-console |
| Credit Card Processor      | 8084 | N/A                              |
| Kafka Broker 1             | 9092 | -                                |
| Kafka Broker 2             | 9094 | -                                |
| Kafka Broker 3             | 9096 | -                                |
| Kafka UI                   | 8088 | -                                |

### H2 Database Access

Each service uses an in-memory H2 database. To access the H2 console for any service:

1. Navigate to `http://localhost:<service-port>/h2-console`
2. Use the following credentials:
   - **JDBC URL**: `jdbc:h2:mem:<service>db` (e.g., `jdbc:h2:mem:ordersdb`)
   - **Username**: `sa`
   - **Password**: (leave empty)

Database names:
- Orders Service: `ordersdb`
- Products Service: `productsdb`
- Payments Service: `paymentsdb`

## 🧪 Testing

### Method 1: Using cURL

1. Start all services as described in [Getting Started](#getting-started)

2. Create a product:
```bash
curl -X POST http://localhost:8081/products \
  -H "Content-Type: application/json" \
  -d '{
    "productName": "Laptop",
    "productPrice": 999.99,
    "productQuantity": 10
  }'
```

3. Place an order (use the productId from step 2):
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "550e8400-e29b-41d4-a716-446655440000",
    "productId": "<product-id-from-step-2>",
    "productQuantity": 2
  }'
```

4. Check order history (use orderId from step 3):
```bash
curl http://localhost:8080/orders/<order-id>/history
```

You should see the order progress through states: `CREATED` → `APPROVED` (on success) or `CREATED` → `REJECTED` (on failure).

### Method 2: Using Kafka UI

1. Access Kafka UI at http://localhost:8088
2. Navigate to **Topics** to see all Kafka topics
3. Monitor messages in real-time as they flow through:
   - `orders-events-topic`
   - `products-commands-topic`
   - `products-events-topic`
   - `payments-commands-topic`
   - `payments-events-topic`
4. View consumer groups and their lag

### Method 3: Using H2 Console

1. Access the H2 console at http://localhost:8080/h2-console (Orders Service)
2. Login with JDBC URL: `jdbc:h2:mem:ordersdb`
3. Query the order history table to see status transitions:
```sql
SELECT * FROM ORDER_HISTORY ORDER BY TIMESTAMP DESC;
```

## 🐳 Docker Compose Options

The project includes two Docker Compose configurations:

1. **docker-compose-kafka-cluster-confluent.yml** (Recommended)
   - Uses Confluent Platform Kafka
   - Includes Kafka UI for monitoring
   - Ports: 9092, 9094, 9096

2. **docker-compose.yml** (Alternative)
   - Uses Bitnami Kafka
   - No UI included
   - Ports: 9091, 9092, 9093

**Note**: The services are configured to use ports 9092, 9094, 9096 by default (Confluent setup).

## 📚 Learn More

- [Saga Pattern](https://microservices.io/patterns/data/saga.html)
- [Spring Boot](https://spring.io/projects/spring-boot)
- [Apache Kafka](https://kafka.apache.org/)
- [Microservices Architecture](https://microservices.io/)

## 📝 License

This project is for demonstration and educational purposes.

---

**Author**: MHB  
**Version**: 1.0.0
