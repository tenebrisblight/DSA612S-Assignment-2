import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "restaurant-db");

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

mongodb:Collection? restaurantsBox = ();
function restaurantsColl() returns mongodb:Collection {
    mongodb:Collection? existing = restaurantsBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("restaurants");
    restaurantsBox = c;
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

mongodb:Collection? inventoryBox = ();
function inventoryColl() returns mongodb:Collection {
    mongodb:Collection? existing = inventoryBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("inventory");
    inventoryBox = c;
    return c;
}

mongodb:Collection? kitchenOrdersBox = ();
function kitchenOrdersColl() returns mongodb:Collection {
    mongodb:Collection? existing = kitchenOrdersBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("kitchen_orders");
    kitchenOrdersBox = c;
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

listener http:Listener httpListener = new (8082);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    // ---------------------------------------------------------- restaurants
    resource function post restaurants(@http:Payload NewRestaurant req) returns map<json>|error {
        string restaurantId = "rest_" + uuid:createType4AsString();
        map<json> openingHours = req.openingHours ?: {};
        mongodb:Collection restaurantsC = restaurantsColl();
        check restaurantsC->insertOne({
            "_id": restaurantId,
            "name": req.name,
            "cuisine": req.cuisine,
            "address": req.address,
            "geo": { "lat": req.lat, "lng": req.lng },
            "isOpen": true,
            "openingHours": <anydata>openingHours,
            "rating": 0.0,
            "createdAt": nowMs()
        });
        return { "restaurantId": restaurantId };
    }

    resource function get restaurants(string? open = ()) returns map<json>[]|error {
        map<json> filter = {};
        if open is string {
            filter["isOpen"] = open == "true";
        }
        Doc[]|error docs = findDocs(restaurantsColl(), filter);
        if docs is error {
            return docs;
        }
        return from var doc in docs select restaurantView(doc);
    }

    resource function get restaurants/[string restaurantId]() returns map<json>|http:NotFound|error {
        mongodb:Collection restaurantsC = restaurantsColl();
        Doc? doc = check restaurantsC->findOne({ "_id": restaurantId });
        if doc is () {
            return <http:NotFound>{ body: "restaurant not found: " + restaurantId };
        }
        return restaurantView(doc);
    }

    resource function put restaurants/[string restaurantId]/hours(@http:Payload map<json> hours)
            returns map<json>|http:NotFound|error {
        mongodb:Collection restaurantsC = restaurantsColl();
        Doc? doc = check restaurantsC->findOne({ "_id": restaurantId });
        if doc is () {
            return <http:NotFound>{ body: "restaurant not found: " + restaurantId };
        }
        _ = check restaurantsC->updateOne({ "_id": restaurantId }, {
            set: { "openingHours": hours }
        });
        return { "restaurantId": restaurantId, "openingHours": hours };
    }

    // ----------------------------------------------------------------- menu
    resource function post restaurants/[string restaurantId]/menu/items(@http:Payload NewMenuItem req)
            returns map<json>|http:NotFound|error {
        mongodb:Collection restaurantsC = restaurantsColl();
        Doc? restaurant = check restaurantsC->findOne({ "_id": restaurantId });
        if restaurant is () {
            return <http:NotFound>{ body: "restaurant not found: " + restaurantId };
        }
        string itemId = "item_" + uuid:createType4AsString();
        mongodb:Collection menuC = menuItemsColl();
        check menuC->insertOne({
            "_id": itemId,
            "restaurantId": restaurantId,
            "name": req.name,
            "description": req.description,
            "price": req.price,
            "category": req.category,
            "available": true
        });
        mongodb:Collection inventoryC = inventoryColl();
        check inventoryC->insertOne({
            "_id": restaurantId + ":" + itemId,
            "restaurantId": restaurantId,
            "itemId": itemId,
            "stockQty": 100,
            "updatedAt": nowMs()
        });
        // Keep the Order Service's menu replica in sync (architecture §1.3).
        check publishMenuUpdated(restaurantId, itemId);
        return { "itemId": itemId };
    }

    resource function get restaurants/[string restaurantId]/menu() returns Doc[]|error {
        return findDocs(menuItemsColl(), { "restaurantId": restaurantId });
    }

    resource function patch restaurants/[string restaurantId]/menu/items/[string itemId](@http:Payload map<json> updates)
            returns map<json>|http:NotFound|error {
        mongodb:Collection menuC = menuItemsColl();
        Doc? doc = check menuC->findOne({
            "_id": itemId,
            "restaurantId": restaurantId
        });
        if doc is () {
            return <http:NotFound>{ body: "menu item not found: " + itemId };
        }
        map<json> setDoc = {};
        string[] editableFields = ["name", "description", "price", "category", "available"];
        foreach string k in editableFields {
            json? value = updates[k];
            if value is () {
                continue;
            }
            setDoc[k] = value;
        }
        _ = check menuC->updateOne({ "_id": itemId }, { set: setDoc });
        check publishMenuUpdated(restaurantId, itemId);
        Doc? updated = check menuC->findOne({ "_id": itemId });
        if updated !is () {
            return menuItemView(updated);
        }
        return { "itemId": itemId };
    }

    // ------------------------------------------------------------ inventory
    resource function get restaurants/[string restaurantId]/inventory() returns Doc[]|error {
        return findDocs(inventoryColl(), { "restaurantId": restaurantId });
    }

    // Real-time stock: running out marks the item unavailable platform-wide
    // via the menu.updated event.
    resource function patch restaurants/[string restaurantId]/inventory/[string itemId](@http:Payload map<json> body)
            returns map<json>|http:NotFound|error {
        mongodb:Collection inventoryC = inventoryColl();
        Doc? inv = check inventoryC->findOne({
            "restaurantId": restaurantId,
            "itemId": itemId
        });
        if inv is () {
            return <http:NotFound>{ body: "inventory record not found for item: " + itemId };
        }
        int stockQty = asInt(body["stockQty"]);
        _ = check inventoryC->updateOne({
            "restaurantId": restaurantId,
            "itemId": itemId
        }, {
            set: { "stockQty": stockQty, "updatedAt": nowMs() }
        });
        if stockQty <= 0 {
            mongodb:Collection menuC = menuItemsColl();
            _ = check menuC->updateOne({ "_id": itemId }, { set: { "available": false } });
            check publishMenuUpdated(restaurantId, itemId);
        }
        return { "itemId": itemId, "stockQty": stockQty };
    }

    // -------------------------------------------------------------- kitchen
    resource function post restaurants/[string restaurantId]/orders/[string orderId]/preparing()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection kitchenC = kitchenOrdersColl();
        Doc? ticket = check kitchenC->findOne({ "_id": orderId });
        if ticket is () {
            return <http:NotFound>{ body: "no kitchen ticket for order " + orderId };
        }
        string status = asString(ticket["status"]);
        if status != "CONFIRMED" {
            return <http:Conflict>{ body: "ticket in status " + status + " cannot move to PREPARING" };
        }
        _ = check kitchenC->updateOne({ "_id": orderId }, {
            set: { "status": "PREPARING", "updatedAt": nowMs() }
        });
        check produceEvent("kitchen.preparing", orderId, { "restaurantId": restaurantId });
        return { "orderId": orderId, "status": "PREPARING" };
    }

    resource function post restaurants/[string restaurantId]/orders/[string orderId]/ready()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection kitchenC = kitchenOrdersColl();
        Doc? ticket = check kitchenC->findOne({ "_id": orderId });
        if ticket is () {
            return <http:NotFound>{ body: "no kitchen ticket for order " + orderId };
        }
        string status = asString(ticket["status"]);
        if status != "PREPARING" {
            return <http:Conflict>{ body: "ticket in status " + status + " cannot move to READY" };
        }
        _ = check kitchenC->updateOne({ "_id": orderId }, {
            set: { "status": "READY", "updatedAt": nowMs() }
        });

        // kitchen.ready carries the pickup location so the Delivery Service
        // can pick the nearest driver (architecture §7.1).
        json pickupLocation = {};
        string restaurantName = "";
        mongodb:Collection restaurantsC = restaurantsColl();
        Doc? restaurant = check restaurantsC->findOne({ "_id": restaurantId });
        if restaurant !is () {
            anydata? geo = restaurant["geo"];
            if geo is map<json> {
                pickupLocation = geo;
            }
            restaurantName = asString(restaurant["name"]);
        }
        check produceEvent("kitchen.ready", orderId, {
            "restaurantId": restaurantId,
            "restaurantName": restaurantName,
            "restaurantLocation": pickupLocation
        });
        return { "orderId": orderId, "status": "READY" };
    }

    resource function post restaurants/[string restaurantId]/orders/[string orderId]/reject()
            returns map<json>|http:NotFound|http:Conflict|error {
        mongodb:Collection kitchenC = kitchenOrdersColl();
        Doc? ticket = check kitchenC->findOne({ "_id": orderId });
        if ticket is () {
            return <http:NotFound>{ body: "no kitchen ticket for order " + orderId };
        }
        string status = asString(ticket["status"]);
        string[] rejectable = ["CONFIRMED", "PREPARING"];
        if !listContains(rejectable, status) {
            return <http:Conflict>{ body: "ticket in status " + status + " cannot be rejected" };
        }
        _ = check kitchenC->updateOne({ "_id": orderId }, {
            set: { "status": "CANCELLED", "updatedAt": nowMs() }
        });
        check produceEvent("kitchen.rejected", orderId, {
            "restaurantId": restaurantId,
            "reason": "kitchen_rejected"
        });
        return { "orderId": orderId, "status": "CANCELLED" };
    }
}

function restaurantView(Doc doc) returns map<json> {
    return {
        "restaurantId": <json>doc["_id"],
        "name": <json>doc["name"],
        "cuisine": <json>doc["cuisine"],
        "address": <json>doc["address"],
        "geo": <json>doc["geo"],
        "isOpen": <json>doc["isOpen"],
        "openingHours": <json>doc["openingHours"],
        "rating": <json>doc["rating"]
    };
}

function menuItemView(Doc doc) returns map<json> {
    return {
        "itemId": <json>doc["_id"],
        "restaurantId": <json>doc["restaurantId"],
        "name": <json>doc["name"],
        "description": <json>doc["description"],
        "price": <json>doc["price"],
        "category": <json>doc["category"],
        "available": <json>doc["available"]
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
