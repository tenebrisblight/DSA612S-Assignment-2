import ballerina/io;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

// Reporting consumer: maintains restaurant and delivery performance stats
// from the event stream (§8.6). pending_orders keeps the per-order facts
// needed to compute durations between events.
//
//   avgPrepMinutes     = kitchen.ready.occurredAt - orders.confirmed.occurredAt
//   avgDeliveryMinutes = delivery.completed.occurredAt - delivery.picked_up.occurredAt
listener kafka:Listener adminListener = new (kafkaServers, {
    groupId: "admin-service",
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.cancelled",
        "payments.completed",
        "kitchen.ready",
        "delivery.picked_up",
        "delivery.completed"
    ],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on adminListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("admin-service: failed to process: ", result.message());
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
        "orders.created" => {
            string restaurantId = asString(env.payload["restaurantId"]);
            mongodb:Collection pendingC = pendingOrdersColl();
            check pendingC->insertOne({
                "_id": env.orderId,
                "restaurantId": restaurantId,
                "total": asFloat(env.payload["total"]),
                "placedAt": env.occurredAt
            });
            return bumpRestaurant(restaurantId, "ordersCount", 1.0);
        }
        "orders.confirmed" => {
            return updatePending(env.orderId, { "confirmedAt": env.occurredAt });
        }
        "payments.completed" => {
            mongodb:Collection pendingC = pendingOrdersColl();
            Doc? pending = check pendingC->findOne({ "_id": env.orderId });
            if pending is () {
                return;
            }
            return bumpRestaurant(asString(pending["restaurantId"]), "revenue", asFloat(env.payload["amount"]));
        }
        "kitchen.ready" => {
            mongodb:Collection pendingC = pendingOrdersColl();
            Doc? pending = check pendingC->findOne({ "_id": env.orderId });
            if pending is () {
                return;
            }
            anydata? confirmedAt = pending["confirmedAt"];
            if confirmedAt is () {
                return;
            }
            float prepMinutes = <float>(env.occurredAt - asInt(confirmedAt)) / 60000.0;
            return addPrepSample(asString(pending["restaurantId"]), prepMinutes);
        }
        "delivery.picked_up" => {
            return updatePending(env.orderId, {
                "pickedUpAt": env.occurredAt,
                "driverId": asString(env.payload["driverId"])
            });
        }
        "delivery.completed" => {
            mongodb:Collection pendingC = pendingOrdersColl();
            Doc? pending = check pendingC->findOne({ "_id": env.orderId });
            if pending is () {
                return;
            }
            anydata? pickedUpAt = pending["pickedUpAt"];
            string driverId = asString(pending["driverId"]);
            _ = check pendingC->deleteOne({ "_id": env.orderId });
            if pickedUpAt is () || driverId == "" {
                return;
            }
            float deliveryMinutes = <float>(env.occurredAt - asInt(pickedUpAt)) / 60000.0;
            return addDeliverySample(driverId, deliveryMinutes);
        }
        "orders.cancelled" => {
            mongodb:Collection pendingC = pendingOrdersColl();
            Doc? pending = check pendingC->findOne({ "_id": env.orderId });
            if pending is () {
                return;
            }
            check bumpRestaurant(asString(pending["restaurantId"]), "cancelledCount", 1.0);
            _ = check pendingC->deleteOne({ "_id": env.orderId });
        }
        _ => {
        }
    }
}

function updatePending(string orderId, map<json> fields) returns error? {
    map<json> setDoc = fields;
    setDoc["updatedAt"] = nowMs();
    mongodb:Collection pendingC = pendingOrdersColl();
    _ = check pendingC->updateOne({ "_id": orderId }, { set: setDoc });
}

function bumpRestaurant(string restaurantId, string statKey, float add) returns error? {
    mongodb:Collection statsC = restaurantStatsColl();
    Doc? stats = check statsC->findOne({ "_id": restaurantId });
    if stats is () {
        Doc base = {
            "_id": restaurantId,
            "ordersCount": 0,
            "cancelledCount": 0,
            "revenue": 0.0,
            "avgPrepMinutes": 0.0,
            "prepSamples": 0
        };
        check statsC->insertOne(base);
        stats = base;
    }
    Doc current = <Doc>stats;
    float updated = asFloat(current[statKey]) + add;
    _ = check statsC->updateOne({ "_id": restaurantId }, {
        set: { [statKey]: updated }
    });
}

// Running average of kitchen prep time per restaurant.
function addPrepSample(string restaurantId, float value) returns error? {
    mongodb:Collection statsC = restaurantStatsColl();
    Doc? stats = check statsC->findOne({ "_id": restaurantId });
    if stats is () {
        Doc base = {
            "_id": restaurantId,
            "ordersCount": 0,
            "cancelledCount": 0,
            "revenue": 0.0,
            "avgPrepMinutes": 0.0,
            "prepSamples": 0
        };
        check statsC->insertOne(base);
        stats = base;
    }
    Doc current = <Doc>stats;
    int samples = asInt(current["prepSamples"]) + 1;
    float avg = (asFloat(current["avgPrepMinutes"]) * <float>(samples - 1) + value) / <float>samples;
    _ = check statsC->updateOne({ "_id": restaurantId }, {
        set: { "avgPrepMinutes": avg, "prepSamples": samples }
    });
}

// Running average of delivery time per driver.
function addDeliverySample(string driverId, float value) returns error? {
    mongodb:Collection statsC = deliveryStatsColl();
    Doc? stats = check statsC->findOne({ "_id": driverId });
    int samples = 0;
    float currentAvg = 0.0;
    if stats !is () {
        samples = asInt(stats["completedCount"]);
        currentAvg = asFloat(stats["avgDeliveryMinutes"]);
    }
    int completedCount = samples + 1;
    float avg = (currentAvg * <float>samples + value) / <float>completedCount;
    Doc? existing = check statsC->findOne({ "_id": driverId });
    if existing is () {
        return check statsC->insertOne({
            "_id": driverId,
            "completedCount": completedCount,
            "avgDeliveryMinutes": avg
        });
    }
    _ = check statsC->updateOne({ "_id": driverId }, {
        set: { "completedCount": completedCount, "avgDeliveryMinutes": avg }
    });
}
