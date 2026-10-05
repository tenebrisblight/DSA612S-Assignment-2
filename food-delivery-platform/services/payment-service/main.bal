import ballerina/http;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "payment-db");

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

mongodb:Collection? paymentsBox = ();
function paymentsColl() returns mongodb:Collection {
    mongodb:Collection? existing = paymentsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("payments");
    paymentsBox = c;
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

listener http:Listener httpListener = new (8084);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    resource function get payments(string? orderId = ()) returns map<json>[]|error {
        map<json> filter = {};
        if orderId is string {
            filter["orderId"] = orderId;
        }
        mongodb:Collection paymentsC = paymentsColl();
        stream<Doc, error?> result = check paymentsC->find(filter);
        map<json>[] docs = [];
        error? iterateError = result.forEach(function(Doc doc) {
            docs.push(paymentView(doc));
        });
        if iterateError is error {
            return iterateError;
        }
        check result.close();
        return docs;
    }

    resource function get payments/[string paymentId]() returns map<json>|http:NotFound|error {
        mongodb:Collection paymentsC = paymentsColl();
        Doc? doc = check paymentsC->findOne({ "_id": paymentId });
        if doc is () {
            return <http:NotFound>{ body: "payment not found: " + paymentId };
        }
        return paymentView(doc);
    }
}

function paymentView(Doc doc) returns map<json> {
    return {
        "paymentId": <json>doc["_id"],
        "orderId": <json>doc["orderId"],
        "customerId": <json>doc["customerId"],
        "amount": <json>doc["amount"],
        "method": <json>doc["method"],
        "status": <json>doc["status"],
        "transactionRef": <json>doc["transactionRef"],
        "reason": <json>doc["reason"]
    };
}
