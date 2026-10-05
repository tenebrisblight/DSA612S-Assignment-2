import ballerina/io;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

final kafka:Producer producer = check new (kafkaServers);

// Kitchen workflow: orders.confirmed creates a kitchen ticket; the kitchen
// drives it via REST (preparing / ready / reject). orders.cancelled withdraws
// the ticket (compensation).
listener kafka:Listener restaurantListener = new (kafkaServers, {
    groupId: "restaurant-service",
    topics: ["orders.confirmed", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on restaurantListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("restaurant-service: failed to process: ", result.message());
            }
        }
    }
}

function handleRecord(kafka:BytesConsumerRecord rec) returns error? {
    string|error decoded = string:fromBytes(rec.value);
    if decoded is error {
        return decoded;
    }
    json|error parsed = decoded.fromJsonString();
    if parsed is error {
        return parsed;
    }
    Envelope|error bound = parsed.cloneWithType(Envelope);
    if bound is error {
        return bound;
    }
    Envelope env = bound;

    // At-least-once delivery: dedupe on eventId before processing (§5.3).
    mongodb:Collection processedC = processedEventsColl();
    Doc? seen = check processedC->findOne({ "_id": env.eventId });
    if seen !is () {
        return;
    }
    check processedC->insertOne({
        "_id": env.eventId,
        "offset": rec.offset,
        "consumedAt": nowMs()
    });

    match env.eventType {
        "orders.confirmed" => {
            return onOrderConfirmed(env);
        }
        "orders.cancelled" => {
            return onOrderCancelled(env);
        }
        _ => {
        }
    }
}

function onOrderConfirmed(Envelope env) returns error? {
    mongodb:Collection kitchenC = kitchenOrdersColl();
    Doc? existing = check kitchenC->findOne({ "_id": env.orderId });
    if existing !is () {
        return;
    }
    check kitchenC->insertOne({
        "_id": env.orderId,
        "restaurantId": asString(env.payload["restaurantId"]),
        "items": <anydata>env.payload["items"],
        "total": asFloat(env.payload["total"]),
        "status": "CONFIRMED",
        "confirmedAt": nowMs()
    });
    io:println("restaurant-service: kitchen ticket created for ", env.orderId);
}

function onOrderCancelled(Envelope env) returns error? {
    mongodb:Collection kitchenC = kitchenOrdersColl();
    Doc? ticket = check kitchenC->findOne({ "_id": env.orderId });
    if ticket is () {
        return;
    }
    _ = check kitchenC->updateOne({ "_id": env.orderId }, {
        set: {
            "status": "CANCELLED",
            "reason": asString(env.payload["reason"]),
            "updatedAt": nowMs()
        }
    });
    io:println("restaurant-service: kitchen ticket cancelled for ", env.orderId);
}

function produceEvent(string topic, string orderId, map<json> payload) returns error? {
    map<json> envelope = {
        "eventId": "evt_" + uuid:createType4AsString(),
        "eventType": topic,
        "occurredAt": nowMs(),
        "orderId": orderId,
        "payload": payload
    };
    return producer->send({
        topic: topic,
        key: orderId.toBytes(),
        value: envelope.toJsonString().toBytes()
    });
}

function publishMenuUpdated(string restaurantId, string itemId) returns error? {
    mongodb:Collection menuC = menuItemsColl();
    Doc? doc = check menuC->findOne({ "_id": itemId });
    if doc is () {
        return;
    }
    return produceEvent("restaurant.menu.updated", itemId, {
        "restaurantId": restaurantId,
        "itemId": itemId,
        "name": <json>doc["name"],
        "description": <json>doc["description"],
        "price": <json>doc["price"],
        "category": <json>doc["category"],
        "available": <json>doc["available"]
    });
}
