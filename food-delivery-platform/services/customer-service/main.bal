import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

final string mongoUrl = envOrDefault("MONGO_URL", "mongodb://localhost:27017");
final string mongoDbName = envOrDefault("MONGO_DB", "customer-db");

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

mongodb:Collection? customersBox = ();
function customersColl() returns mongodb:Collection {
    mongodb:Collection? existing = customersBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("customers");
    customersBox = c;
    return c;
}

mongodb:Collection? addressesBox = ();
function addressesColl() returns mongodb:Collection {
    mongodb:Collection? existing = addressesBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("addresses");
    addressesBox = c;
    return c;
}

mongodb:Collection? orderHistoryBox = ();
function orderHistoryColl() returns mongodb:Collection {
    mongodb:Collection? existing = orderHistoryBox;
    if existing is mongodb:Collection {
        return existing;
    }
    mongodb:Database d = db();
    mongodb:Collection c = checkpanic d->getCollection("order_history");
    orderHistoryBox = c;
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

listener http:Listener httpListener = new (8081);

service / on httpListener {

    resource function get health() returns string {
        return "OK";
    }

    // ------------------------------------------------------------- accounts
    resource function post customers(@http:Payload map<json> req) returns map<json>|http:Conflict|error {
        string email = asString(req["email"]);
        mongodb:Collection customersC = customersColl();
        Doc? existing = check customersC->findOne({ "email": email });
        if existing !is () {
            return <http:Conflict>{ body: "email already registered: " + email };
        }
        string customerId = "cust_" + uuid:createType4AsString();
        check customersC->insertOne({
            "_id": customerId,
            "name": asString(req["name"]),
            "email": email,
            "phone": asString(req["phone"]),
            "createdAt": nowMs()
        });
        return { "customerId": customerId };
    }

    resource function get customers(string? email = ()) returns map<json>[]|error {
        map<json> filter = {};
        if email is string {
            filter["email"] = email;
        }
        Doc[]|error docs = findDocs(customersColl(), filter);
        if docs is error {
            return docs;
        }
        return from var doc in docs select customerView(doc);
    }

    resource function get customers/[string customerId]() returns map<json>|http:NotFound|error {
        mongodb:Collection customersC = customersColl();
        Doc? doc = check customersC->findOne({ "_id": customerId });
        if doc is () {
            return <http:NotFound>{ body: "customer not found: " + customerId };
        }
        return customerView(doc);
    }

    resource function put customers/[string customerId](@http:Payload map<json> req)
            returns map<json>|http:NotFound|error {
        mongodb:Collection customersC = customersColl();
        Doc? doc = check customersC->findOne({ "_id": customerId });
        if doc is () {
            return <http:NotFound>{ body: "customer not found: " + customerId };
        }
        map<json> setDoc = {};
        json? name = req["name"];
        if name is string {
            setDoc["name"] = name;
        }
        json? phone = req["phone"];
        if phone is string {
            setDoc["phone"] = phone;
        }
        _ = check customersC->updateOne({ "_id": customerId }, { set: setDoc });
        return { "customerId": customerId, "updated": true };
    }

    // ------------------------------------------------------------ addresses
    resource function post customers/[string customerId]/addresses(@http:Payload map<json> req)
            returns map<json>|http:NotFound|error {
        mongodb:Collection customersC = customersColl();
        Doc? doc = check customersC->findOne({ "_id": customerId });
        if doc is () {
            return <http:NotFound>{ body: "customer not found: " + customerId };
        }
        mongodb:Collection addressesC = addressesColl();
        boolean isDefault = req["isDefault"] is true;
        if isDefault {
            // Only one default address per customer.
            Doc[]|error existing = findDocs(addressesC, { "customerId": customerId });
            if existing is Doc[] {
                foreach Doc address in existing {
                    _ = check addressesC->updateOne({ "_id": <json>address["_id"] }, {
                        set: { "isDefault": false }
                    });
                }
            }
        }
        string addressId = "addr_" + uuid:createType4AsString();
        check addressesC->insertOne({
            "_id": addressId,
            "customerId": customerId,
            "label": asString(req["label"]),
            "street": asString(req["street"]),
            "city": asString(req["city"]),
            "geo": { "lat": asFloat(req["lat"]), "lng": asFloat(req["lng"]) },
            "isDefault": isDefault
        });
        return { "addressId": addressId };
    }

    resource function get customers/[string customerId]/addresses() returns map<json>[]|error {
        Doc[]|error docs = findDocs(addressesColl(), { "customerId": customerId });
        if docs is error {
            return docs;
        }
        return from var doc in docs select addressView(doc);
    }

    // ------------------------------------------------------- order history
    // A read model maintained by consuming orders.* topics — the Customer
    // Service never calls the Order Service (architecture §1.3, CQRS-lite).
    resource function get customers/[string customerId]/orders() returns map<json>[]|error {
        Doc[]|error docs = findDocs(orderHistoryColl(), { "customerId": customerId });
        if docs is error {
            return docs;
        }
        return from var doc in docs
            order by asInt(doc["placedAt"]) descending
            select orderHistoryView(doc);
    }
}

function customerView(Doc doc) returns map<json> {
    return {
        "customerId": <json>doc["_id"],
        "name": <json>doc["name"],
        "email": <json>doc["email"],
        "phone": <json>doc["phone"]
    };
}

function addressView(Doc doc) returns map<json> {
    return {
        "addressId": <json>doc["_id"],
        "label": <json>doc["label"],
        "street": <json>doc["street"],
        "city": <json>doc["city"],
        "geo": <json>doc["geo"],
        "isDefault": <json>doc["isDefault"]
    };
}

function orderHistoryView(Doc doc) returns map<json> {
    return {
        "orderId": <json>doc["_id"],
        "restaurantId": <json>doc["restaurantId"],
        "itemCount": <json>doc["itemCount"],
        "total": <json>doc["total"],
        "status": <json>doc["status"],
        "placedAt": <json>doc["placedAt"],
        "updatedAt": <json>doc["updatedAt"]
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
