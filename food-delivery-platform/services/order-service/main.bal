import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "order-db");
final float deliveryFee = 15.0;

final mongodb:Client baseClient = check new ({ connection: mongoUrl });

// Module-level initializers cannot contain actions (remote calls), so the
// Database/Collection handles are created lazily via checkpanic accessors.
mongodb:Database? dbBox = ();
function db() returns mongodb:Database {
    mongodb:Database? existing = dbBox;
    if existing is mongodb:Database {
        return existing;
    }
    mongodb:Database d = checkpanic baseClient->getDatabase(mongoDbName);
    dbBox = d;
    return d;
}

mongodb:Collection? ordersBox = ();
function ordersColl() returns mongodb:Collection {
    mongodb:Collection? existing = ordersBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("orders");
    ordersBox = c;
    return c;
}

mongodb:Collection? menuItemsBox = ();
function menuItemsColl() returns mongodb:Collection {
    mongodb:Collection? existing = menuItemsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("menu_items");
    menuItemsBox = c;
    return c;
}

mongodb:Collection? statusHistoryBox = ();
function statusHistoryColl() returns mongodb:Collection {
    mongodb:Collection? existing = statusHistoryBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("order_status_history");
    statusHistoryBox = c;
    return c;
}

mongodb:Collection? processedEventsBox = ();
function processedEventsColl() returns mongodb:Collection {
    mongodb:Collection? existing = processedEventsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("processed_events");
    processedEventsBox = c;
    return c;
}

listener http:Listener httpListener = new (8083);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    // Place an order: validate against the local menu replica, price
    // server-side, persist as CREATED, publish orders.created.
    resource function post orders(@http:Payload NewOrderRequest req)
            returns map<json>|http:BadRequest|error {
        if req.items.length() == 0 {
            return <http:BadRequest>{ body: "order must contain at least one item" };
        }

        mongodb:Collection menuC = menuItemsColl();
        map<json>[] lines = [];
        float subtotal = 0.0;
        foreach NewOrderItem requested in req.items {
            Doc? menuItem = check menuC->findOne({
                "_id": requested.itemId,
                "restaurantId": req.restaurantId
            });
            if menuItem is () {
                return <http:BadRequest>{ body: "unknown menu item: " + requested.itemId };
            }
            if !asBool(menuItem["available"]) {
                return <http:BadRequest>{ body: "item unavailable: " + requested.itemId };
            }
            float unitPrice = asFloat(menuItem["price"]);
            float lineTotal = unitPrice * requested.qty;
            subtotal += lineTotal;
            lines.push({
                "itemId": requested.itemId,
                "name": asString(menuItem["name"]),
                "unitPrice": unitPrice,
                "qty": requested.qty,
                "lineTotal": lineTotal
            });
        }

        string orderId = "ord_" + uuid:createType4AsString();
        int now = nowMs();
        string paymentMethod = req.paymentMethod ?: "CARD";
        float total = subtotal + deliveryFee;

        mongodb:Collection ordersC = ordersColl();
        check ordersC->insertOne({
            "_id": orderId,
            "customerId": req.customerId,
            "restaurantId": req.restaurantId,
            "status": "CREATED",
            "deliveryAddress": <anydata>req.deliveryAddress,
            "items": <anydata>lines,
            "subtotal": subtotal,
            "deliveryFee": deliveryFee,
            "total": total,
            "paymentMethod": paymentMethod,
            "placedAt": now,
            "updatedAt": now
        });

        check produceEvent("orders.created", orderId, {
            "customerId": req.customerId,
            "restaurantId": req.restaurantId,
            "deliveryAddress": req.deliveryAddress,
            "items": lines,
            "subtotal": subtotal,
            "deliveryFee": deliveryFee,
            "total": total,
            "paymentMethod": paymentMethod
        });

        return {
            "orderId": orderId,
            "status": "CREATED",
            "items": lines,
            "subtotal": subtotal,
            "deliveryFee": deliveryFee,
            "total": total,
            "placedAt": now
        };
    }

    resource function get orders(string? customerId = (), string? restaurantId = (), string? status = ())
            returns map<json>[]|error {
        map<json> filter = {};
        if customerId is string {
            filter["customerId"] = customerId;
        }
        if restaurantId is string {
            filter["restaurantId"] = restaurantId;
        }
        if status is string {
            filter["status"] = status;
        }
        Doc[]|error docs = findDocs(ordersColl(), filter);
        if docs is error {
            return docs;
        }
        return from var doc in docs select orderView(doc);
    }

    resource function get orders/[string orderId]() returns map<json>|http:NotFound|error {
        mongodb:Collection ordersC = ordersColl();
        Doc? doc = check ordersC->findOne({ "_id": orderId });
        if doc is () {
            return <http:NotFound>{ body: "order not found: " + orderId };
        }
        return orderView(doc);
    }

    // Customer cancel: validated against the state machine, then broadcast
    // via orders.cancelled (refund/notify fan-out) by applyTransition.
    resource function post orders/[string orderId]/cancel()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection ordersC = ordersColl();
        Doc? doc = check ordersC->findOne({ "_id": orderId });
        if doc is () {
            return <http:NotFound>{ body: "order not found: " + orderId };
        }
        string status = asString(doc["status"]);
        string[] cancellable = ["CREATED", "CONFIRMED", "PREPARING", "READY"];
        if !listContains(cancellable, status) {
            return <http:Conflict>{ body: "order in status " + status + " cannot be cancelled" };
        }
        boolean ok = check applyTransition(orderId, "CANCELLED", "customer_cancelled",
            "evt_" + uuid:createType4AsString(), {});
        if !ok {
            return <http:Conflict>{ body: "order can no longer be cancelled" };
        }
        return { "orderId": orderId, "status": "CANCELLED", "reason": "customer_cancelled" };
    }

    resource function get orders/[string orderId]/history() returns map<json>[]|error {
        Doc[]|error docs = findDocs(statusHistoryColl(), { "orderId": orderId });
        if docs is error {
            return docs;
        }
        return from var doc in docs
            order by asInt(doc["occurredAt"]) ascending
            select {
                "from": <json>doc["from"],
                "to": <json>doc["to"],
                "reason": <json>doc["reason"],
                "occurredAt": <json>doc["occurredAt"]
            };
    }
}

function orderView(Doc doc) returns map<json> {
    return {
        "orderId": <json>doc["_id"],
        "customerId": <json>doc["customerId"],
        "restaurantId": <json>doc["restaurantId"],
        "status": <json>doc["status"],
        "items": <json>doc["items"],
        "subtotal": <json>doc["subtotal"],
        "deliveryFee": <json>doc["deliveryFee"],
        "total": <json>doc["total"],
        "deliveryAddress": <json>doc["deliveryAddress"],
        "paymentMethod": <json>doc["paymentMethod"],
        "driverId": <json>doc["driverId"],
        "placedAt": <json>doc["placedAt"]
    };
}

function findDocs(mongodb:Collection coll, map<json> filter) returns Doc[]|error {
    stream<Doc, error?> result = check coll->find(filter);
    Doc[] docs = [];
    error? iterateError = result.forEach(function(Doc doc) {
        docs.push(doc);
    });
    if iterateError is error {
        return iterateError;
    }
    check result.close();
    return docs;
}
