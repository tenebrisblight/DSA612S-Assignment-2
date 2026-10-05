import ballerina/io;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

final kafka:Producer producer = check new (kafkaServers);

// Payment Service: consumes orders.created (trigger) and orders.cancelled
// (compensation). Processing is simulated; a paymentMethod of TEST_DECLINE
// deterministically fails so the compensation path can be demoed.
listener kafka:Listener paymentListener = new (kafkaServers, {
    groupId: "payment-service",
    topics: ["orders.created", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on paymentListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("payment-service: failed to process: ", result.message());
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
        "orders.cancelled" => {
            return onOrderCancelled(env);
        }
        _ => {
        }
    }
}

function onOrderCreated(Envelope env) returns error? {
    string orderId = env.orderId;
    map<json> payload = env.payload;
    string method = asString(payload["paymentMethod"]);
    float amount = asFloat(payload["total"]);
    string paymentId = "pay_" + uuid:createType4AsString();
    int now = nowMs();

    mongodb:Collection paymentsC = paymentsColl();
    check paymentsC->insertOne({
        "_id": paymentId,
        "orderId": orderId,
        "customerId": asString(payload["customerId"]),
        "amount": amount,
        "method": method,
        "status": "PENDING",
        "createdAt": now,
        "updatedAt": now
    });

    io:println("payment-service: processing ", paymentId, " for ", orderId, " amount=", amount);

    if method == "TEST_DECLINE" {
        _ = check paymentsC->updateOne({ "_id": paymentId }, {
            set: { "status": "FAILED", "reason": "test_decline", "updatedAt": nowMs() }
        });
        check produceEvent("payments.failed", orderId, {
            "paymentId": paymentId,
            "customerId": asString(payload["customerId"]),
            "amount": amount,
            "reason": "test_decline"
        });
        return;
    }

    string transactionRef = "TXN-" + uuid:createType4AsString().substring(0, 8);
    _ = check paymentsC->updateOne({ "_id": paymentId }, {
        set: {
            "status": "COMPLETED",
            "transactionRef": transactionRef,
            "completedAt": nowMs(),
            "updatedAt": nowMs()
        }
    });
    check produceEvent("payments.completed", orderId, {
        "paymentId": paymentId,
        "customerId": asString(payload["customerId"]),
        "amount": amount,
        "transactionRef": transactionRef,
        "method": method
    });
}

// Compensation: refund completed payments, void ones never settled.
function onOrderCancelled(Envelope env) returns error? {
    mongodb:Collection paymentsC = paymentsColl();
    Doc? paymentResult = check paymentsC->findOne({ "orderId": env.orderId });
    if paymentResult is () {
        io:println("payment-service: no payment for cancelled order ", env.orderId);
        return;
    }
    string paymentId = asString(paymentResult["_id"]);
    if asString(paymentResult["status"]) == "COMPLETED" {
        _ = check paymentsC->updateOne({ "_id": paymentId }, {
            set: {
                "status": "REFUNDED",
                "reason": asString(env.payload["reason"]),
                "updatedAt": nowMs()
            }
        });
        io:println("payment-service: refunded ", paymentId);
        return;
    }
    _ = check paymentsC->updateOne({ "_id": paymentId }, {
        set: { "status": "FAILED", "reason": "order_cancelled", "updatedAt": nowMs() }
    });
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
