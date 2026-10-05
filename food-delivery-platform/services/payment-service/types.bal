import ballerina/os;
import ballerina/time;

// Kafka event envelope — identical shape on every topic (architecture §5.1).
public type Envelope record {|
    string eventId;
    string eventType;
    int occurredAt;
    string orderId;
    map<json> payload;
|};

public type PaymentRecord record {
    string _id;
    string orderId;
    string customerId;
    float amount;
    string method;
    string status;
    string reason?;
    string transactionRef?;
    int createdAt;
    int updatedAt;
};

// Generic mongo document (default findOne/find target type).
type Doc record {| anydata...; |};

// config + json coercion helpers -----------------------------------------
function envOrDefault(string name, string dflt) returns string {
    string v = os:getEnv(name);
    if v == "" {
        return dflt;
    }
    return v;
}

function nowMs() returns int {
    return time:utcNow()[0] * 1000;
}

function asString(json|anydata? value) returns string {
    if value is string {
        return value;
    }
    if value is () {
        return "";
    }
    return value.toString();
}

function asFloat(json|anydata? value) returns float {
    if value is float {
        return value;
    }
    if value is int {
        return <float>value;
    }
    if value is decimal {
        return <float>value;
    }
    if value is string {
        float|error parsed = float:fromString(value);
        return parsed is float ? parsed : 0.0;
    }
    return 0.0;
}
