#!/usr/bin/env python3
"""server.py -- gRPC Food Ordering Server.

    python3 server.py localhost:50051

Holds the restaurant catalogue and all order state in memory, validates
order-status transitions, and pushes status changes to subscribed customers
over a server-streaming RPC.

Concurrency model
-----------------
gRPC runs each RPC on a thread from a pool, so handlers run truly in
parallel. All shared state (orders, subscriber lists, the id counter) is
guarded by one lock. Critical sections are tiny (dict lookups/updates), so a
single lock is simpler and safe; no I/O happens while it is held.

Subscribers are per-order lists of queue.Queue. A status change pushes one
OrderUpdate into every queue for that order; the streaming handler blocks on
its queue and yields whatever arrives. Pushing happens under the lock, but
the (potentially slow) network send happens outside it in the stream thread.
"""
import queue
import sys
import threading
from concurrent import futures

import grpc

import food_ordering_pb2 as pb
import food_ordering_pb2_grpc as pb_grpc

# ---------------------------------------------------------------------------
# Static data
# ---------------------------------------------------------------------------
RESTAURANTS = {
    "Pizza House": {
        "Margherita Pizza": 250,
        "Farmhouse Pizza": 350,
        "Garlic Bread": 150,
    },
    "Burger Point": {
        "Veg Burger": 180,
        "Cheese Burger": 220,
        "French Fries": 120,
    },
}

# Allowed state machine: current -> set of next states
TRANSITIONS = {
    pb.PLACED:    {pb.ACCEPTED, pb.CANCELLED},
    pb.ACCEPTED:  {pb.PREPARING},
    pb.PREPARING: {pb.READY},
    pb.READY:     set(),
    pb.CANCELLED: set(),
}
TERMINAL = {pb.READY, pb.CANCELLED}

STATUS_NAME = pb.OrderStatus.Name


class Order:
    __slots__ = ("order_id", "restaurant", "items", "total", "status")

    def __init__(self, order_id, restaurant, items, total):
        self.order_id = order_id
        self.restaurant = restaurant
        self.items = items          # list of (name, qty)
        self.total = total
        self.status = pb.PLACED

    def to_status_response(self):
        return pb.OrderStatusResponse(
            order_id=self.order_id,
            restaurant_name=self.restaurant,
            items=[pb.OrderItem(name=n, quantity=q) for n, q in self.items],
            total=self.total,
            status=self.status,
        )


class FoodOrderingServicer(pb_grpc.FoodOrderingServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._orders = {}            # order_id -> Order
        self._subscribers = {}       # order_id -> [queue.Queue, ...]
        self._next_id = 101

    # ---- helpers -----------------------------------------------------------
    def _get_order_or_abort(self, order_id, context):
        """Caller must hold the lock."""
        order = self._orders.get(order_id)
        if order is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Order %s does not exist." % order_id)
        return order

    def _publish(self, order, message):
        """Push the order's current status to every subscriber. Lock held."""
        update = pb.OrderUpdate(order_id=order.order_id, status=order.status, message=message)
        for q in self._subscribers.get(order.order_id, []):
            q.put(update)

    def _apply_transition(self, order, new_status, context):
        """Validate and apply a state change. Lock held."""
        if new_status not in TRANSITIONS[order.status]:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Invalid order state transition: %s -> %s"
                % (STATUS_NAME(order.status), STATUS_NAME(new_status)),
            )
        order.status = new_status
        self._publish(order, "Order %s : %s" % (order.order_id, STATUS_NAME(new_status)))

    # ---- RPCs --------------------------------------------------------------
    def ListRestaurants(self, request, context):
        resp = pb.RestaurantResponse()                  #new empty list
        for name, menu in RESTAURANTS.items():
            r = resp.restaurants.add(name=name)         #Adds a new resturant message inside the list, sets name=name and then gives 
            for item, price in menu.items():            
                r.items.add(name=item, price=price)     
        return resp

    def PlaceOrder(self, request, context):
        menu = RESTAURANTS.get(request.restaurant_name)
        if menu is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "Restaurant '%s' does not exist." % request.restaurant_name)
        if not request.items:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Order must contain at least one item.")

        items, total = [], 0
        for it in request.items:
            if it.name not in menu:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              "Item '%s' is not available at %s." % (it.name, request.restaurant_name))
            if it.quantity <= 0:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              "Quantity for '%s' must be positive." % it.name)
            items.append((it.name, it.quantity))
            total += menu[it.name] * it.quantity

        with self._lock:
            order_id = "O%d" % self._next_id
            self._next_id += 1
            order = Order(order_id, request.restaurant_name, items, total)
            self._orders[order_id] = order
        return pb.OrderResponse(order_id=order_id, total=total, status=pb.PLACED)

    def GetOrderStatus(self, request, context):
        with self._lock:
            order = self._get_order_or_abort(request.order_id, context)
            return order.to_status_response()

    def UpdateOrderStatus(self, request, context):
        with self._lock:
            order = self._get_order_or_abort(request.order_id, context)
            if order.restaurant != request.restaurant_name:
                context.abort(grpc.StatusCode.PERMISSION_DENIED,
                              "Order %s belongs to %s, not %s."
                              % (order.order_id, order.restaurant, request.restaurant_name))
            if request.new_status == pb.CANCELLED:
                context.abort(grpc.StatusCode.PERMISSION_DENIED,
                              "Restaurants cannot cancel orders.")
            self._apply_transition(order, request.new_status, context)
            return pb.Acknowledge(success=True,
                                  message="Order %s : %s" % (order.order_id, STATUS_NAME(order.status)),
                                  status=order.status)

    def CancelOrder(self, request, context):
        with self._lock:
            order = self._get_order_or_abort(request.order_id, context)
            if order.status != pb.PLACED:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "Order %s cannot be cancelled: it is already %s."
                              % (order.order_id, STATUS_NAME(order.status)))
            self._apply_transition(order, pb.CANCELLED, context)
            return pb.Acknowledge(success=True,
                                  message="Order %s : CANCELLED" % order.order_id,
                                  status=order.status)

    def GetPendingOrders(self, request, context):
        if request.restaurant_name not in RESTAURANTS:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "Restaurant '%s' does not exist." % request.restaurant_name)
        with self._lock:
            pending = [o.to_status_response() for o in self._orders.values()
                       if o.restaurant == request.restaurant_name and o.status not in TERMINAL]
        return pb.PendingOrdersResponse(orders=pending)

    def SubscribeToOrderUpdates(self, request, context):
        q = queue.Queue()
        with self._lock:
            order = self._get_order_or_abort(request.order_id, context)
            # Send the current state first so the subscriber starts in sync.
            q.put(pb.OrderUpdate(order_id=order.order_id, status=order.status,
                                 message="Order %s : %s" % (order.order_id, STATUS_NAME(order.status))))
            if order.status in TERMINAL:
                # Nothing more will ever happen; deliver the snapshot and finish.
                yield q.get()
                return
            self._subscribers.setdefault(order.order_id, []).append(q)

        try:
            while context.is_active():
                try:
                    update = q.get(timeout=0.5)   # wake up periodically to notice disconnects
                except queue.Empty:
                    continue
                yield update
                if update.status in TERMINAL:
                    break
        finally:
            with self._lock:
                subs = self._subscribers.get(request.order_id, [])
                if q in subs:
                    subs.remove(q)


def serve(address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_FoodOrderingServiceServicer_to_server(FoodOrderingServicer(), server)
    server.add_insecure_port(address)
    server.start()
    print("[Server] Food Ordering Server listening on %s" % address)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[Server] Shutting down.")
        server.stop(grace=1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 server.py <host:port>   e.g. python3 server.py localhost:50051")
        sys.exit(1)
    serve(sys.argv[1])
