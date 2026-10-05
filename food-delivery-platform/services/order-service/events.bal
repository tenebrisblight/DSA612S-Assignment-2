import ballerina/io;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

final kafka:Producer producer = check new (kafkaServers);

// The Order Service consumes every service's facts and is the ONLY writer of
// order status (architecture §1.2). restaurant.menu.updated maintains the
// local menu replica used to validate/price orders at creation time.
listener kafka:Listener orderEventsListener = new (kafkaServers, {
    groupId: "order-service",
    topics: [
        "payments.completed",
        "payments.failed",
        "kitchen.preparing",
        "kitchen.ready",
        "kitchen.rejected",
        "delivery.assigned",
        "delivery.picked_up",
        "delivery.completed",
        "delivery.failed",
        "restaurant.menu.updated"
    ],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on orderEventsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("order-service: failed to process: ", result.message());
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
    return processEvent(env);
}

function processEvent(Envelope env) returns error? {
    io:println("order-service: ", env.eventType, " for ", env.orderId);
    match env.eventType {
        "payments.completed" => {
            return runTransition(env.orderId, "CONFIRMED", "payment_completed", env.eventId, {
                "paymentId": asString(env.payload["paymentId"]),
                "transactionRef": asString(env.payload["transactionRef"])
            });
        }
        "payments.failed" => {
            return runTransition(env.orderId, "CANCELLED", "payment_failed", env.eventId, {});
        }
        "kitchen.preparing" => {
            return runTransition(env.orderId, "PREPARING", "kitchen_started", env.eventId, {});
        }
        "kitchen.ready" => {
            return runTransition(env.orderId, "READY", "kitchen_ready", env.eventId, {});
        }
        "kitchen.rejected" => {
            return runTransition(env.orderId, "CANCELLED", "kitchen_rejected", env.eventId, {});
        }
        "delivery.assigned" => {
            // Not a state change — just record the driver on the order.
            mongodb:Collection ordersC = ordersColl();
            Doc? doc = check ordersC->findOne({ "_id": env.orderId });
            if doc !is () {
                _ = check ordersC->updateOne({ "_id": env.orderId }, {
                    set: {
                        "driverId": asString(env.payload["driverId"]),
                        "updatedAt": nowMs()
                    }
                });
            }
        }
        "delivery.picked_up" => {
            return runTransition(env.orderId, "OUT_FOR_DELIVERY", "driver_picked_up", env.eventId, {});
        }
        "delivery.completed" => {
            return runTransition(env.orderId, "DELIVERED", "delivery_completed", env.eventId, {});
        }
        "restaurant.menu.updated" => {
            return onMenuUpdated(env.payload);
        }
        _ => {
            // delivery.failed and unknown event types need no state change.
        }
    }
}

// Valid transitions of the order state machine (architecture §6). Anything
// not listed is rejected — no state skipping.
final map<string[]> allowedTransitions = {
    "CREATED": ["CONFIRMED", "CANCELLED"],
    "CONFIRMED": ["PREPARING", "CANCELLED"],
    "PREPARING": ["READY", "CANCELLED"],
    "READY": ["OUT_FOR_DELIVERY", "CANCELLED"],
    "OUT_FOR_DELIVERY": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": []
};

// Idempotent, validated transition. Returns true if the order actually moved.
// Always broadcasts orders.status.changed; additionally emits the canonical
// orders.confirmed / orders.cancelled events used for compensation fan-out.
function applyTransition(string orderId, string to, string reason, string eventId, map<json> extraFields)
        returns boolean|error {
    mongodb:Collection processedC = processedEventsColl();
    Doc? seen = check processedC->findOne({ "_id": eventId });
    if seen !is () {
        return false;
    }

    mongodb:Collection ordersC = ordersColl();
    Doc? docResult = check ordersC->findOne({ "_id": orderId });
    if docResult is () {
        io:println("order-service: event for unknown order ", orderId);
        return false;
    }
    OrderRecord ord = check docResult.cloneWithType(OrderRecord);

    string fromState = ord.status;
    string[] allowed = allowedTransitions[fromState] ?: [];
    if !listContains(allowed, to) {
        io:println("order-service: rejected transition ", orderId, " ", fromState, " -> ", to, " (", reason, ")");
        return false;
    }

    map<json> setDoc = { "status": to, "updatedAt": nowMs() };
    foreach string key in extraFields.keys() {
        setDoc[key] = extraFields[key];
    }
    _ = check ordersC->updateOne({ "_id": orderId }, { set: setDoc });

    mongodb:Collection historyC = statusHistoryColl();
    check historyC->insertOne({
        "_id": eventId,
        "orderId": orderId,
        "from": fromState,
        "to": to,
        "reason": reason,
        "occurredAt": nowMs()
    });

    check produceEvent("orders.status.changed", orderId, {
        "from": fromState,
        "to": to,
        "reason": reason
    });

    if to == "CONFIRMED" {
        check produceEvent("orders.confirmed", orderId, {
            "customerId": ord.customerId,
            "restaurantId": ord.restaurantId,
            "items": ord.items,
            "total": ord.total,
            "paymentId": asString(extraFields["paymentId"]),
            "transactionRef": asString(extraFields["transactionRef"])
        });
    }

    if to == "CANCELLED" {
        // Compensation (mini-saga): refund if the payment had gone through.
        string[] paidStates = ["CONFIRMED", "PREPARING", "READY"];
        check produceEvent("orders.cancelled", orderId, {
            "customerId": ord.customerId,
            "restaurantId": ord.restaurantId,
            "driverId": ord.driverId,
            "reason": reason,
            "refund": listContains(paidStates, fromState)
        });
    }

    return true;
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

// Upsert the menu replica document carried by restaurant.menu.updated.
function onMenuUpdated(map<json> payload) returns error? {
    string itemId = asString(payload["itemId"]);
    map<json> doc = {
        "_id": itemId,
        "restaurantId": asString(payload["restaurantId"]),
        "name": asString(payload["name"]),
        "price": asFloat(payload["price"]),
        "category": asString(payload["category"]),
        "available": asBool(payload["available"])
    };
    mongodb:Collection menuC = menuItemsColl();
    Doc? existing = check menuC->findOne({ "_id": itemId });
    if existing is () {
        check menuC->insertOne(<map<anydata>>doc);
    return;
    }
    map<json> setDoc = doc;
    _ = setDoc.remove("_id");
    _ = check menuC->updateOne({ "_id": itemId }, { set: setDoc });
}

// Event-side wrapper: the boolean from applyTransition is only meaningful to
// the REST cancel path (main.bal); consumers just propagate failures.
function runTransition(string orderId, string to, string reason, string eventId, map<json> extraFields) returns error? {
    boolean|error moved = applyTransition(orderId, to, reason, eventId, extraFields);
    if moved is error {
        return moved;
    }
}
