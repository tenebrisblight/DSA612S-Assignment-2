import ballerina/io;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

final kafka:Producer producer = check new (kafkaServers);

// Delivery flow: orders.created captures the drop-off address; kitchen.ready
// captures the pick-up location and triggers nearest-driver assignment.
listener kafka:Listener deliveryListener = new (kafkaServers, {
    groupId: "delivery-service",
    topics: ["orders.created", "kitchen.ready", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on deliveryListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("delivery-service: failed to process: ", result.message());
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
            return onOrderCreated(env);
        }
        "kitchen.ready" => {
            return onKitchenReady(env);
        }
        "orders.cancelled" => {
            return onOrderCancelled(env);
        }
        _ => {
        }
    }
}

function onOrderCreated(Envelope env) returns error? {
    mongodb:Collection deliveriesC = deliveriesColl();
    check deliveriesC->insertOne({
        "_id": env.orderId,
        "restaurantId": asString(env.payload["restaurantId"]),
        "customerId": asString(env.payload["customerId"]),
        "status": "UNASSIGNED",
        "dropoffLocation": <anydata>env.payload["deliveryAddress"],
        "createdAt": nowMs()
    });
    io:println("delivery-service: delivery record created for ", env.orderId);
}

function onKitchenReady(Envelope env) returns error? {
    mongodb:Collection deliveriesC = deliveriesColl();
    Doc? delivery = check deliveriesC->findOne({ "_id": env.orderId });
    if delivery is () {
        io:println("delivery-service: kitchen.ready for unknown order ", env.orderId);
        return;
    }
    _ = check deliveriesC->updateOne({ "_id": env.orderId }, {
        set: {
            "pickupLocation": env.payload["restaurantLocation"],
            "restaurantName": asString(env.payload["restaurantName"]),
            "updatedAt": nowMs()
        }
    });
    boolean assigned = check tryAssign(env.orderId);
    if !assigned {
        io:println("delivery-service: no driver available for ", env.orderId,
            " — retry via POST /deliveries/", env.orderId, "/assign");
    }
}

// Nearest AVAILABLE driver by great-circle distance; marks the driver BUSY
// and broadcasts delivery.assigned. Returns false when no driver is free.
function tryAssign(string orderId) returns boolean|error {
    mongodb:Collection deliveriesC = deliveriesColl();
    Doc? delivery = check deliveriesC->findOne({ "_id": orderId });
    if delivery is () {
        return false;
    }
    anydata? pickupJson = delivery["pickupLocation"];
    if !(pickupJson is map<json>) {
        return false;
    }
    map<json> pickupLoc = pickupJson;
    float pickupLat = asFloat(pickupLoc["lat"]);
    float pickupLng = asFloat(pickupLoc["lng"]);

    mongodb:Collection driversC = driversColl();
    Doc[]|error available = findDocs(driversC, { "status": "AVAILABLE" });
    if available is error {
        return available;
    }
    string? bestDriver = ();
    float bestDistance = 999999999.0;
    foreach Doc driver in available {
        anydata? locJson = driver["currentLocation"];
        if !(locJson is map<json>) {
            continue;
        }
        map<json> loc = locJson;
        float distance = haversineKm(pickupLat, pickupLng, asFloat(loc["lat"]), asFloat(loc["lng"]));
        if distance < bestDistance {
            bestDistance = distance;
            bestDriver = asString(driver["_id"]);
        }
    }
    if bestDriver is () {
        return false;
    }
    string driverId = bestDriver;
    int etaMinutes = <int>(bestDistance / 25.0 * 60.0) + 1;

    _ = check driversC->updateOne({ "_id": driverId }, {
        set: { "status": "BUSY" }
    });
    _ = check deliveriesC->updateOne({ "_id": orderId }, {
        set: {
            "status": "ASSIGNED",
            "driverId": driverId,
            "assignedAt": nowMs(),
            "etaMinutes": etaMinutes,
            "updatedAt": nowMs()
        }
    });

    string driverName = "";
    Doc? driverDoc = check driversC->findOne({ "_id": driverId });
    if driverDoc !is () {
        driverName = asString(driverDoc["name"]);
    }
    check produceEvent("delivery.assigned", orderId, {
        "driverId": driverId,
        "driverName": driverName,
        "customerId": asString(delivery["customerId"]),
        "etaMinutes": etaMinutes
    });
    io:println("delivery-service: assigned ", driverId, " to ", orderId, " (eta ", etaMinutes, " min)");
    return true;
}

// Compensation: release the driver if one was assigned.
function onOrderCancelled(Envelope env) returns error? {
    mongodb:Collection deliveriesC = deliveriesColl();
    Doc? delivery = check deliveriesC->findOne({ "_id": env.orderId });
    if delivery is () {
        return;
    }
    string driverId = asString(delivery["driverId"]);
    _ = check deliveriesC->updateOne({ "_id": env.orderId }, {
        set: {
            "status": "FAILED",
            "reason": asString(env.payload["reason"]),
            "updatedAt": nowMs()
        }
    });
    if driverId != "" {
        mongodb:Collection driversC = driversColl();
        _ = check driversC->updateOne({ "_id": driverId }, {
            set: { "status": "AVAILABLE" }
        });
    }
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
