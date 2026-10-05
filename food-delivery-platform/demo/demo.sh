#!/usr/bin/env bash
# End-to-end demo of the food delivery platform.
# Run after `docker compose up -d --build` (optionally pass the host, default localhost):
#   ./demo/demo.sh [host]
set -e

HOST="${1:-localhost}"
CUSTOMER="http://$HOST:8081"
RESTAURANT="http://$HOST:8082"
ORDER="http://$HOST:8083"
PAYMENT="http://$HOST:8084"
DELIVERY="http://$HOST:8085"
NOTIF="http://$HOST:8086"
ADMIN="http://$HOST:8087"

step() { echo; echo "== $1 =="; }
extract() { sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -1; }
jsonget() { sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\([^,}]*\).*/\1/p" | head -1; }

wait_status() {
  local id="$1" target="$2" status=""
  for _ in $(seq 1 40); do
    status=$(curl -s "$ORDER/orders/$id" | extract status)
    if [ "$status" = "$target" ]; then echo "  order $id -> $status"; return 0; fi
    sleep 1
  done
  echo "  TIMEOUT: order $id never reached $target (last: ${status:-none})"; return 1
}

step "Waiting for all services to become healthy"
for port in 8081 8082 8083 8084 8085 8086 8087; do
  until curl -sf "http://$HOST:$port/health" > /dev/null; do sleep 2; done
  echo "  :$port OK"
done

step "Customer registers (idempotent: look up by email, create if new)"
CUSTOMER_ID=$(curl -s "$CUSTOMER/customers?email=naledi@example.com" | extract customerId)
if [ -z "$CUSTOMER_ID" ]; then
  CUSTOMER_ID=$(curl -s -X POST "$CUSTOMER/customers" -H 'Content-Type: application/json' \
    -d '{"name":"Naledi Shipanga","email":"naledi@example.com","phone":"+264 81 123 4567"}' | extract customerId)
fi
echo "  customerId: $CUSTOMER_ID"

step "Customer adds a delivery address"
curl -s -X POST "$CUSTOMER/customers/$CUSTOMER_ID/addresses" -H 'Content-Type: application/json' \
  -d '{"label":"Home","street":"12 Independence Ave","city":"Windhoek","lat":-22.5597,"lng":17.0832,"isDefault":true}'
echo

step "Restaurant onboards and publishes a menu item"
RESTAURANT_ID=$(curl -s -X POST "$RESTAURANT/restaurants" -H 'Content-Type: application/json' \
  -d '{"name":"Kapana Kingdom","cuisine":"Namibian BBQ","address":"Single Quarters, Katutura","lat":-22.4907,"lng":17.0550}' | extract restaurantId)
echo "  restaurantId: $RESTAURANT_ID"
ITEM_ID=$(curl -s -X POST "$RESTAURANT/restaurants/$RESTAURANT_ID/menu/items" -H 'Content-Type: application/json' \
  -d '{"name":"Kapana Combo","description":"Beef kapana with spice and pap","price":65.0,"category":"Grill"}' | extract itemId)
echo "  itemId: $ITEM_ID  (orders.validate against this via the event-fed menu replica)"

step "Driver registers"
DRIVER_ID=$(curl -s -X POST "$DELIVERY/drivers" -H 'Content-Type: application/json' \
  -d '{"name":"Tomas Hainyeko","phone":"+264 81 987 6543","lat":-22.51,"lng":17.06}' | extract driverId)
echo "  driverId: $DRIVER_ID"

step "Customer places an order (happy path)"
ORDER_ID=$(curl -s -X POST "$ORDER/orders" -H 'Content-Type: application/json' \
  -d "{\"customerId\":\"$CUSTOMER_ID\",\"restaurantId\":\"$RESTAURANT_ID\",\"items\":[{\"itemId\":\"$ITEM_ID\",\"qty\":2}],\"deliveryAddress\":{\"street\":\"12 Independence Ave\",\"city\":\"Windhoek\",\"lat\":-22.5597,\"lng\":17.0832}}" | extract orderId)
echo "  orderId: $ORDER_ID"

echo "  (payment processes asynchronously over Kafka)"
wait_status "$ORDER_ID" "CONFIRMED"

step "Kitchen starts cooking, then marks the food ready"
curl -s -X POST "$RESTAURANT/restaurants/$RESTAURANT_ID/orders/$ORDER_ID/preparing"; echo
wait_status "$ORDER_ID" "PREPARING"
curl -s -X POST "$RESTAURANT/restaurants/$RESTAURANT_ID/orders/$ORDER_ID/ready"; echo
wait_status "$ORDER_ID" "READY"

step "Delivery: nearest driver auto-assigned on kitchen.ready"
ASSIGNED=""
for _ in $(seq 1 30); do
  ASSIGNED=$(curl -s "$DELIVERY/deliveries/$ORDER_ID" | extract driverId)
  if [ -n "$ASSIGNED" ]; then break; fi
  sleep 1
done
echo "  delivery: $(curl -s "$DELIVERY/deliveries/$ORDER_ID")"

step "Driver picks up and delivers"
curl -s -X POST "$DELIVERY/deliveries/$ORDER_ID/picked_up"; echo
wait_status "$ORDER_ID" "OUT_FOR_DELIVERY"
curl -s -X POST "$DELIVERY/deliveries/$ORDER_ID/completed"; echo
wait_status "$ORDER_ID" "DELIVERED"

step "Final order state"
curl -s "$ORDER/orders/$ORDER_ID"; echo
echo "  status history:"
curl -s "$ORDER/orders/$ORDER_ID/history"; echo
echo "  payment record:"
curl -s "$PAYMENT/payments?orderId=$ORDER_ID"; echo

step "Customer order history (read model fed by events)"
curl -s "$CUSTOMER/customers/$CUSTOMER_ID/orders"; echo

step "Notifications for the customer (multi-channel fan-out)"
curl -s "$NOTIF/notifications?recipientId=$CUSTOMER_ID"; echo

step "Admin reports (event-fed read models)"
echo "  restaurant stats:"
curl -s "$ADMIN/admin/reports/restaurants"; echo
echo "  delivery stats:"
curl -s "$ADMIN/admin/reports/deliveries"; echo

step "Compensation demo: order with TEST_DECLINE payment method"
ORDER2_ID=$(curl -s -X POST "$ORDER/orders" -H 'Content-Type: application/json' \
  -d "{\"customerId\":\"$CUSTOMER_ID\",\"restaurantId\":\"$RESTAURANT_ID\",\"items\":[{\"itemId\":\"$ITEM_ID\",\"qty\":1}],\"deliveryAddress\":{\"street\":\"12 Independence Ave\",\"city\":\"Windhoek\",\"lat\":-22.5597,\"lng\":17.0832},\"paymentMethod\":\"TEST_DECLINE\"}" | extract orderId)
echo "  orderId: $ORDER2_ID"
wait_status "$ORDER2_ID" "CANCELLED"
echo "  payment record (simulated auth declined, no charge):"
curl -s "$PAYMENT/payments?orderId=$ORDER2_ID"; echo
echo "  notifications:"
curl -s "$NOTIF/notifications?recipientId=$CUSTOMER_ID&orderId=$ORDER2_ID"; echo

echo
echo "Demo complete. Explore kafka-ui at http://localhost:8090 and mongo-express at http://localhost:8091"
