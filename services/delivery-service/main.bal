import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "delivery-db");

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

mongodb:Collection? driversBox = ();
function driversColl() returns mongodb:Collection {
    mongodb:Collection? existing = driversBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("drivers");
    driversBox = c;
    return c;
}

mongodb:Collection? deliveriesBox = ();
function deliveriesColl() returns mongodb:Collection {
    mongodb:Collection? existing = deliveriesBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("deliveries");
    deliveriesBox = c;
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

listener http:Listener httpListener = new (8085);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    // --------------------------------------------------------------- drivers
    resource function post drivers(@http:Payload map<json> req) returns map<json>|error {
        string driverId = "drv_" + uuid:createType4AsString();
        float lat = asFloat(req["lat"]);
        float lng = asFloat(req["lng"]);
        if lat == 0.0 && lng == 0.0 {
            // Default to central Windhoek when no GPS position is given.
            lat = -22.5597;
            lng = 17.0832;
        }
        mongodb:Collection driversC = driversColl();
        check driversC->insertOne({
            "_id": driverId,
            "name": asString(req["name"]),
            "phone": asString(req["phone"]),
            "status": "AVAILABLE",
            "currentLocation": { "lat": lat, "lng": lng },
            "lastLocationAt": nowMs()
        });
        return { "driverId": driverId };
    }

    resource function get drivers(string? status = ()) returns map<json>[]|error {
        map<json> filter = {};
        if status is string {
            filter["status"] = status;
        }
        Doc[]|error docs = findDocs(driversColl(), filter);
        if docs is error {
            return docs;
        }
        return from var doc in docs select driverView(doc);
    }

    // BUSY is managed by the service (assignment/completion); drivers flip
    // themselves between AVAILABLE and OFFLINE.
    resource function patch drivers/[string driverId]/status(@http:Payload map<json> body)
            returns map<json>|http:NotFound|http:BadRequest|error {
        string status = asString(body["status"]);
        string[] valid = ["AVAILABLE", "OFFLINE"];
        if !listContains(valid, status) {
            return <http:BadRequest>{ body: "status must be AVAILABLE or OFFLINE" };
        }
        mongodb:Collection driversC = driversColl();
        Doc? doc = check driversC->findOne({ "_id": driverId });
        if doc is () {
            return <http:NotFound>{ body: "driver not found: " + driverId };
        }
        _ = check driversC->updateOne({ "_id": driverId }, {
            set: { "status": status, "updatedAt": nowMs() }
        });
        return { "driverId": driverId, "status": status };
    }

    // GPS ping — feeds the driver-location bonus (live map overlay).
    resource function patch drivers/[string driverId]/location(@http:Payload map<json> body)
            returns map<json>|http:NotFound|error {
        mongodb:Collection driversC = driversColl();
        Doc? doc = check driversC->findOne({ "_id": driverId });
        if doc is () {
            return <http:NotFound>{ body: "driver not found: " + driverId };
        }
        map<json> location = { "lat": asFloat(body["lat"]), "lng": asFloat(body["lng"]) };
        _ = check driversC->updateOne({ "_id": driverId }, {
            set: { "currentLocation": location, "lastLocationAt": nowMs() }
        });
        return { "driverId": driverId, "currentLocation": location };
    }

    // ------------------------------------------------------------ deliveries
    // Manual (re)assignment — used when no driver was available at kitchen.ready.
    resource function post deliveries/[string orderId]/assign()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection deliveriesC = deliveriesColl();
        Doc? delivery = check deliveriesC->findOne({ "_id": orderId });
        if delivery is () {
            return <http:NotFound>{ body: "no delivery record for order " + orderId };
        }
        if asString(delivery["status"]) != "UNASSIGNED" {
            return <http:Conflict>{ body: "delivery already in status " + asString(delivery["status"]) };
        }
        boolean assigned = check tryAssign(orderId);
        if !assigned {
            return <http:Conflict>{ body: "no available drivers" };
        }
        Doc? updated = check deliveriesC->findOne({ "_id": orderId });
        if updated !is () {
            return deliveryView(updated);
        }
        return { "orderId": orderId, "status": "ASSIGNED" };
    }

    resource function post deliveries/[string orderId]/picked_up()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection deliveriesC = deliveriesColl();
        Doc? delivery = check deliveriesC->findOne({ "_id": orderId });
        if delivery is () {
            return <http:NotFound>{ body: "no delivery record for order " + orderId };
        }
        string status = asString(delivery["status"]);
        if status != "ASSIGNED" {
            return <http:Conflict>{ body: "delivery in status " + status + " cannot be picked up" };
        }
        string driverId = asString(delivery["driverId"]);
        _ = check deliveriesC->updateOne({ "_id": orderId }, {
            set: { "status": "PICKED_UP", "pickedUpAt": nowMs(), "updatedAt": nowMs() }
        });
        check produceEvent("delivery.picked_up", orderId, {
            "driverId": driverId,
            "customerId": asString(delivery["customerId"])
        });
        return { "orderId": orderId, "status": "PICKED_UP", "driverId": driverId };
    }

    resource function post deliveries/[string orderId]/completed()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection deliveriesC = deliveriesColl();
        Doc? delivery = check deliveriesC->findOne({ "_id": orderId });
        if delivery is () {
            return <http:NotFound>{ body: "no delivery record for order " + orderId };
        }
        string status = asString(delivery["status"]);
        if status != "PICKED_UP" {
            return <http:Conflict>{ body: "delivery in status " + status + " cannot be completed" };
        }
        string driverId = asString(delivery["driverId"]);
        _ = check deliveriesC->updateOne({ "_id": orderId }, {
            set: { "status": "COMPLETED", "deliveredAt": nowMs(), "updatedAt": nowMs() }
        });
        mongodb:Collection driversC = driversColl();
        _ = check driversC->updateOne({ "_id": driverId }, {
            set: { "status": "AVAILABLE" }
        });
        check produceEvent("delivery.completed", orderId, {
            "driverId": driverId,
            "customerId": asString(delivery["customerId"]),
            "deliveredAt": nowMs()
        });
        return { "orderId": orderId, "status": "COMPLETED", "driverId": driverId };
    }

    resource function get deliveries(string? driverId = (), string? status = ()) returns map<json>[]|error {
        map<json> filter = {};
        if driverId is string {
            filter["driverId"] = driverId;
        }
        if status is string {
            filter["status"] = status;
        }
        Doc[]|error docs = findDocs(deliveriesColl(), filter);
        if docs is error {
            return docs;
        }
        return from var doc in docs select deliveryView(doc);
    }

    resource function get deliveries/[string orderId]() returns map<json>|http:NotFound|error {
        mongodb:Collection deliveriesC = deliveriesColl();
        Doc? doc = check deliveriesC->findOne({ "_id": orderId });
        if doc is () {
            return <http:NotFound>{ body: "no delivery record for order " + orderId };
        }
        return deliveryView(doc);
    }
}

function driverView(Doc doc) returns map<json> {
    return {
        "driverId": <json>doc["_id"],
        "name": <json>doc["name"],
        "status": <json>doc["status"],
        "currentLocation": <json>doc["currentLocation"]
    };
}

function deliveryView(Doc doc) returns map<json> {
    return {
        "orderId": <json>doc["_id"],
        "restaurantId": <json>doc["restaurantId"],
        "driverId": <json>doc["driverId"],
        "status": <json>doc["status"],
        "pickupLocation": <json>doc["pickupLocation"],
        "dropoffLocation": <json>doc["dropoffLocation"],
        "etaMinutes": <json>doc["etaMinutes"]
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

// Great-circle distance in km — nearest-driver selection and ETA (bonus:
// route optimization baseline).
function haversineKm(float lat1, float lng1, float lat2, float lng2) returns float {
    float rad = 3.141592653589793 / 180.0;
    float dLat = (lat2 - lat1) * rad;
    float dLng = (lng2 - lng1) * rad;
    float a = float:pow(float:sin(dLat / 2.0), 2.0)
        + float:cos(lat1 * rad) * float:cos(lat2 * rad) * float:pow(float:sin(dLng / 2.0), 2.0);
    return 2.0 * 6371.0 * float:asin(float:sqrt(a));
}
