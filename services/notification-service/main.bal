import ballerina/http;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "notification-db");

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

mongodb:Collection? notificationsBox = ();
function notificationsColl() returns mongodb:Collection {
    mongodb:Collection? existing = notificationsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("notifications");
    notificationsBox = c;
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

listener http:Listener httpListener = new (8086);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    // Notification inbox — the UI polls this per recipient (customer app,
    // kitchen screen, driver app).
    resource function get notifications(string? recipientId = (), string? orderId = ())
            returns map<json>[]|error {
        map<json> filter = {};
        if recipientId is string {
            filter["recipientId"] = recipientId;
        }
        if orderId is string {
            filter["orderId"] = orderId;
        }
        Doc[]|error docs = findDocs(notificationsColl(), filter);
        if docs is error {
            return docs;
        }
        return from var doc in docs
            order by asInt(doc["createdAt"]) descending
            select notificationView(doc);
    }
}

function notificationView(Doc doc) returns map<json> {
    return {
        "notificationId": <json>doc["_id"],
        "orderId": <json>doc["orderId"],
        "recipientId": <json>doc["recipientId"],
        "recipientRole": <json>doc["recipientRole"],
        "message": <json>doc["message"],
        "channel": <json>doc["channel"],
        "createdAt": <json>doc["createdAt"]
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
