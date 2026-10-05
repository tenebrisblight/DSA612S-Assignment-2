import ballerina/io;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

final string kafkaServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

// Wide consumer: subscribes to every actor-relevant domain topic in its own
// consumer group, so it sees the full event stream independently (§3).
// orders.status.changed is deliberately excluded — the specific topics below
// already cover each transition, and consuming both would duplicate alerts.
listener kafka:Listener notificationListener = new (kafkaServers, {
    groupId: "notification-service",
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.cancelled",
        "payments.completed",
        "payments.failed",
        "kitchen.preparing",
        "kitchen.ready",
        "kitchen.rejected",
        "delivery.assigned",
        "delivery.picked_up",
        "delivery.completed",
        "delivery.failed"
    ],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service kafka:Service on notificationListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleRecord(rec);
            if result is error {
                io:println("notification-service: failed to process: ", result.message());
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

    map<anydata>[] notifications = buildNotifications(env);
    mongodb:Collection notificationsC = notificationsColl();
    foreach map<anydata> notification in notifications {
        check notificationsC->insertOne(notification);
    }
    if notifications.length() > 0 {
        io:println("notification-service: ", notifications.length(), " alert(s) for ", env.eventType);
    }
}

// One event can alert several actors (customer + restaurant, or driver).
function buildNotifications(Envelope env) returns map<anydata>[] {
    map<json> p = env.payload;
    map<anydata>[] out = [];
    string customerId = asString(p["customerId"]);
    string restaurantId = asString(p["restaurantId"]);
    match env.eventType {
        "orders.created" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Your order has been placed and is awaiting payment."));
        }
        "payments.completed" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Payment of N$" + asFloat(p["amount"]).toString() + " received. The restaurant is confirming your order."));
        }
        "payments.failed" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Payment declined (" + asString(p["reason"]) + "). Your order has been cancelled."));
        }
        "orders.confirmed" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "The restaurant confirmed your order and will start preparing it."));
            out.push(newNotification(env, restaurantId, "RESTAURANT",
                "New confirmed order " + env.orderId + " — accept it in the kitchen screen."));
        }
        "kitchen.preparing" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "The kitchen has started preparing your order."));
        }
        "kitchen.ready" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Your order is ready. A driver is being assigned."));
            out.push(newNotification(env, restaurantId, "RESTAURANT",
                "Order " + env.orderId + " is ready for handover."));
        }
        "kitchen.rejected" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "The restaurant could not fulfil your order. You will be refunded."));
        }
        "delivery.assigned" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Driver " + asString(p["driverName"]) + " is on the way (ETA "
                    + asInt(p["etaMinutes"]).toString() + " min)."));
            out.push(newNotification(env, asString(p["driverId"]), "DRIVER",
                "You have a new pickup: order " + env.orderId + "."));
        }
        "delivery.picked_up" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Your order has been picked up and is out for delivery."));
        }
        "delivery.completed" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Your order has been delivered. Enjoy!"));
        }
        "orders.cancelled" => {
            out.push(newNotification(env, customerId, "CUSTOMER",
                "Your order was cancelled (" + asString(p["reason"]) + ")."));
            out.push(newNotification(env, restaurantId, "RESTAURANT",
                "Order " + env.orderId + " was cancelled."));
        }
        _ => {
            // delivery.failed and anything else without a template.
        }
    }
    return out;
}

function newNotification(Envelope env, string recipientId, string role, string message) returns map<anydata> {
    return {
        "_id": "ntf_" + uuid:createType4AsString(),
        "eventId": env.eventId,
        "orderId": env.orderId,
        "recipientId": recipientId,
        "recipientRole": role,
        "message": message,
        "channel": "EMAIL",
        "createdAt": nowMs()
    };
}
