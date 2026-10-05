import ballerina/io;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

// Read-model consumer: keeps each customer's order history up to date purely
// from events — no synchronous coupling to the Order Service.
listener kafka:Listener customerListener = new (kafkaServers, {
    groupId: "customer-service",
    topics: ["orders.created", "orders.status.changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on customerListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("customer-service: failed to process: ", result.message());
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

    mongodb:Collection historyC = orderHistoryColl();
    match env.eventType {
        "orders.created" => {
            return check historyC->insertOne({
                "_id": env.orderId,
                "customerId": asString(env.payload["customerId"]),
                "restaurantId": asString(env.payload["restaurantId"]),
                "itemCount": arrayLength(env.payload["items"]),
                "total": asFloat(env.payload["total"]),
                "status": "CREATED",
                "placedAt": env.occurredAt,
                "updatedAt": env.occurredAt
            });
        }
        "orders.status.changed" => {
            _ = check historyC->updateOne({ "_id": env.orderId }, {
                set: {
                    "status": asString(env.payload["to"]),
                    "updatedAt": nowMs()
                }
            });
        }
        _ => {
        }
    }
}
