# 📈 Event-Driven Trading System

A event-driven trading system designed to demonstrate how modern trading platforms are built using **event streaming, microservices, and real-time communication**.

This project focuses on **decoupled services**, **asynchronous processing**, and **real-time updates**, similar to architectures used in real-world institutional trading systems.

---

## 🧠 Overview

The system processes inquiry events (inquiries and positions) using an **event-driven architecture**.  
Services communicate via **Kafka based event bus**, and updates are pushed to a **real-time UI using WebSockets**.

This project is intended for:
- Demonstrating event-driven system design
- Demonstrating trading-system architecture
---

## 🧩 Key Features

- ✅ Event-driven microservice architecture
- ✅ Kafka-based event bus
- ✅ Redis based position management
- ✅ Protobuf-based message serialization
- ✅ WebSocket-based real-time UI updates
- ✅ Modular services (easy to extend)
- ✅ Mock trading logic (auto-trader)

---

## 🏛 Architecture

<img width="563" height="521" alt="Event Driven Trading System" src="https://github.com/user-attachments/assets/38fa3552-44c9-49a1-9bf7-6954d3e96d53" />

## 🎯 Modules

### Kafka Event bus
Acts as the decoupled messaging backbone that streams trade inquiries and execution events between microservices in real-time.

### Position Store - Redis
Provides a high-speed, in-memory data layer for sub-millisecond retrieval and updates of current trading positions.

### Inquiry Generator 
Orchestrates the start of the workflow by creating and publishing trade inquiries.

### Position Service
Responsible for the business logic of tracking position and enriching inquiries with position.

### Auto Trader
A mock algo engine which is reponsible for accpting and rejecting inquiries. In real life this will depend on maket signals and various other facts in deciding the action.

### Websocket Backend (Websocket Server)
Websocket server based on Vert.x, responsible for publishing protobuf based inquiries to UI.

### Web(Inquiry Dashboard)

<img width="1904" height="814" alt="image" src="https://github.com/user-attachments/assets/3dfcb9bc-dfd3-4bd7-a0d8-c6ba38c44dcd" />

### common

Protobufs and models are defined here.

### Integration Test

A simple integration test is included in the project to make sure position is calculated  accurately and message is sequenced.


## How to Run
### Prerequisite

### Run

### Run integration test




