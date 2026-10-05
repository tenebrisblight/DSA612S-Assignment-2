import ballerina/http;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "admin-db");

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

mongodb:Collection? restaurantStatsBox = ();
function restaurantStatsColl() returns mongodb:Collection {
    mongodb:Collection? existing = restaurantStatsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("restaurant_stats");
    restaurantStatsBox = c;
    return c;
}

mongodb:Collection? deliveryStatsBox = ();
function deliveryStatsColl() returns mongodb:Collection {
    mongodb:Collection? existing = deliveryStatsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("delivery_stats");
    deliveryStatsBox = c;
    return c;
}

mongodb:Collection? pendingOrdersBox = ();
function pendingOrdersColl() returns mongodb:Collection {
    mongodb:Collection? existing = pendingOrdersBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("pending_orders");
    pendingOrdersBox = c;
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

listener http:Listener httpListener = new (8087);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    // Reporting read model — populated entirely by the event consumer in
    // events.bal; the Admin Service queries no other service's database.
    resource function get admin/reports/restaurants() returns Doc[]|error {
        return findDocs(restaurantStatsColl(), {});
    }

    resource function get admin/reports/restaurants/[string restaurantId]()
            returns map<json>|http:NotFound|error {
        mongodb:Collection statsC = restaurantStatsColl();
        Doc? doc = check statsC->findOne({ "_id": restaurantId });
        if doc is () {
            return <http:NotFound>{ body: "no stats yet for restaurant: " + restaurantId };
        }
        return restaurantStatsView(doc);
    }

    resource function get admin/reports/deliveries() returns Doc[]|error {
        return findDocs(deliveryStatsColl(), {});
    }
}

function restaurantStatsView(Doc doc) returns map<json> {
    return {
        "restaurantId": <json>doc["_id"],
        "ordersCount": <json>doc["ordersCount"],
        "cancelledCount": <json>doc["cancelledCount"],
        "revenue": <json>doc["revenue"],
        "avgPrepMinutes": <json>doc["avgPrepMinutes"]
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
