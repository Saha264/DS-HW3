#!/usr/bin/env python3
"""test_system.py -- automated end-to-end test.

Starts the server on a free port, then drives it with real gRPC stubs:
the happy path, streaming updates, concurrent clients, and every exception
case listed in the assignment. Prints PASS/FAIL per check.

    python3 test_system.py
"""
import socket
import subprocess
import sys
import threading
import time

import grpc

import food_ordering_pb2 as pb
import food_ordering_pb2_grpc as pb_grpc

S = pb.OrderStatus.Name
fails = 0


def check(cond, label):
    global fails
    print(("PASS  " if cond else "FAIL  ") + label)
    if not cond:
        fails += 1


def expect_error(fn, code, label):
    try:
        fn()
        check(False, label + " (no error raised)")
    except grpc.RpcError as e:
        check(e.code() == code, "%s -> %s: %s" % (label, e.code().name, e.details()))


def free_port():
    s = socket.socket(); s.bind(("localhost", 0)); p = s.getsockname()[1]; s.close(); return p


port = free_port()
addr = "localhost:%d" % port
srv = subprocess.Popen([sys.executable, "server.py", addr],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    channel = grpc.insecure_channel(addr)
    grpc.channel_ready_future(channel).result(timeout=10)
    stub = pb_grpc.FoodOrderingServiceStub(channel)

    print("--- 1. Listing restaurants")
    r = stub.ListRestaurants(pb.RestaurantRequest())
    check({x.name for x in r.restaurants} == {"Pizza House", "Burger Point"}, "two restaurants listed")

    print("--- 2. Placing an order")
    o = stub.PlaceOrder(pb.OrderRequest(restaurant_name="Pizza House", items=[
        pb.OrderItem(name="Margherita Pizza", quantity=1), pb.OrderItem(name="Garlic Bread", quantity=2)]))
    check(o.order_id == "O101" and o.total == 550 and o.status == pb.PLACED,
          "order O101 total 550 PLACED (got %s %d %s)" % (o.order_id, o.total, S(o.status)))

    print("--- 3/4. Streaming updates while restaurant processes the order")
    received = []
    def subscriber():
        for u in stub.SubscribeToOrderUpdates(pb.OrderRequest(order_id=o.order_id)):
            received.append(u.status)
    t = threading.Thread(target=subscriber); t.start(); time.sleep(0.3)

    for st in (pb.ACCEPTED, pb.PREPARING, pb.READY):
        a = stub.UpdateOrderStatus(pb.OrderStatusUpdate(order_id=o.order_id, restaurant_name="Pizza House", new_status=st))
        check(a.status == st, "restaurant moved order to %s" % S(st))
        time.sleep(0.1)
    t.join(timeout=3)
    check(received == [pb.PLACED, pb.ACCEPTED, pb.PREPARING, pb.READY],
          "subscriber received %s and stream closed" % [S(x) for x in received])
    check(stub.GetOrderStatus(pb.OrderStatusRequest(order_id="O101")).status == pb.READY, "GetOrderStatus shows READY")

    print("--- 5. Concurrent clients (20 threads placing orders + 20 racing to accept)")
    ids = []
    lock = threading.Lock()
    def place(i):
        rest = "Pizza House" if i % 2 else "Burger Point"
        item = "Farmhouse Pizza" if i % 2 else "Veg Burger"
        res = stub.PlaceOrder(pb.OrderRequest(restaurant_name=rest, items=[pb.OrderItem(name=item, quantity=1)]))
        with lock: ids.append(res.order_id)
    ts = [threading.Thread(target=place, args=(i,)) for i in range(20)]
    [x.start() for x in ts]; [x.join() for x in ts]
    check(len(set(ids)) == 20, "20 concurrent orders got 20 unique ids")

    target = [i for i in ids if stub.GetOrderStatus(pb.OrderStatusRequest(order_id=i)).restaurant_name == "Pizza House"][0]
    outcomes = []
    def race_accept():
        try:
            stub.UpdateOrderStatus(pb.OrderStatusUpdate(order_id=target, restaurant_name="Pizza House", new_status=pb.ACCEPTED))
            with lock: outcomes.append("ok")
        except grpc.RpcError as e:
            with lock: outcomes.append(e.code().name)
    ts = [threading.Thread(target=race_accept) for _ in range(20)]
    [x.start() for x in ts]; [x.join() for x in ts]
    check(outcomes.count("ok") == 1 and outcomes.count("FAILED_PRECONDITION") == 19,
          "20 racing accepts: exactly 1 succeeded, 19 rejected (%s)" % dict((k, outcomes.count(k)) for k in set(outcomes)))

    print("--- 6. Exception handling with gRPC status codes")
    expect_error(lambda: stub.PlaceOrder(pb.OrderRequest(restaurant_name="Taco Town", items=[pb.OrderItem(name="x", quantity=1)])),
                 grpc.StatusCode.NOT_FOUND, "non-existent restaurant")
    expect_error(lambda: stub.PlaceOrder(pb.OrderRequest(restaurant_name="Pizza House", items=[pb.OrderItem(name="Sushi", quantity=1)])),
                 grpc.StatusCode.NOT_FOUND, "unavailable food item")
    expect_error(lambda: stub.GetOrderStatus(pb.OrderStatusRequest(order_id="O999")),
                 grpc.StatusCode.NOT_FOUND, "non-existent order")
    expect_error(lambda: stub.CancelOrder(pb.CancelOrderRequest(order_id=target)),
                 grpc.StatusCode.FAILED_PRECONDITION, "cancel an already-accepted order")
    expect_error(lambda: stub.UpdateOrderStatus(pb.OrderStatusUpdate(order_id=target, restaurant_name="Burger Point", new_status=pb.PREPARING)),
                 grpc.StatusCode.PERMISSION_DENIED, "restaurant updates another restaurant's order")
    expect_error(lambda: stub.UpdateOrderStatus(pb.OrderStatusUpdate(order_id="O101", restaurant_name="Pizza House", new_status=pb.PREPARING)),
                 grpc.StatusCode.FAILED_PRECONDITION, "invalid transition READY -> PREPARING")
    expect_error(lambda: stub.UpdateOrderStatus(pb.OrderStatusUpdate(order_id=target, restaurant_name="Pizza House", new_status=pb.READY)),
                 grpc.StatusCode.FAILED_PRECONDITION, "invalid transition ACCEPTED -> READY (skipping PREPARING)")

    print("--- 7. Cancel path")
    c = stub.PlaceOrder(pb.OrderRequest(restaurant_name="Burger Point", items=[pb.OrderItem(name="French Fries", quantity=3)]))
    got = []
    t = threading.Thread(target=lambda: got.extend(u.status for u in stub.SubscribeToOrderUpdates(pb.OrderRequest(order_id=c.order_id))))
    t.start(); time.sleep(0.2)
    a = stub.CancelOrder(pb.CancelOrderRequest(order_id=c.order_id))
    t.join(timeout=3)
    check(a.status == pb.CANCELLED and got == [pb.PLACED, pb.CANCELLED], "PLACED -> CANCELLED, subscriber notified, stream closed")
    p = stub.GetPendingOrders(pb.PendingOrdersRequest(restaurant_name="Burger Point"))
    check(c.order_id not in [x.order_id for x in p.orders], "cancelled order no longer in pending list")

    print()
    print("ALL CHECKS PASSED" if fails == 0 else "%d CHECK(S) FAILED" % fails)
finally:
    srv.terminate(); srv.wait()
sys.exit(1 if fails else 0)
