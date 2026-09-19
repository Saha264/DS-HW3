#!/usr/bin/env python3
"""customer.py -- Customer CLI for the Food Ordering system.

    python3 customer.py localhost:50051

Commands
  restaurants                                   list restaurants and menus
  order <restaurant> "<item>" <qty> ["<item>" <qty> ...]
  status <order_id>                             one-shot status check
  track <order_id>                              live updates (runs in background)
  cancel <order_id>
  help
  exit

`track` uses a server-streaming RPC on a background thread, so updates are
printed as they arrive while you keep typing other commands.
"""
import shlex
import sys
import threading

import grpc

import food_ordering_pb2 as pb
import food_ordering_pb2_grpc as pb_grpc

STATUS_NAME = pb.OrderStatus.Name

MENU = """
Commands:
  1. restaurants                                        List Restaurants
  2. order <restaurant> "<item>" <qty> [...]            Place Order
  3. status <order_id>                                  Check Order Status
  4. track <order_id>                                   Track Order (live updates)
  5. cancel <order_id>                                  Cancel Order
  6. exit
"""


def rpc_error(e):
    print("[Error] %s (%s)" % (e.details(), e.code().name))


def parse_order_args(tokens):
    """Split 'Pizza House "Margherita Pizza" 1 "Garlic Bread" 2' into
    (restaurant, [(item, qty), ...]). Items are read as (name, int) pairs
    from the END; whatever is left at the front is the restaurant name."""
    items = []
    while len(tokens) >= 2 and tokens[-1].isdigit():
        qty = int(tokens.pop())
        name = tokens.pop()
        items.insert(0, (name, qty))
    restaurant = " ".join(tokens).strip()
    return restaurant, items


class CustomerClient:
    def __init__(self, address):
        self.channel = grpc.insecure_channel(address)
        self.stub = pb_grpc.FoodOrderingServiceStub(self.channel)
        self.track_threads = []

    # ---- commands ----------------------------------------------------------
    def list_restaurants(self):
        try:
            resp = self.stub.ListRestaurants(pb.RestaurantRequest())
        except grpc.RpcError as e:
            return rpc_error(e)
        print("[Server]")
        for i, r in enumerate(resp.restaurants, 1):
            print("%d. %s" % (i, r.name))
            width = max(len(it.name) for it in r.items)
            for it in r.items:
                print("   - %-*s : %d" % (width, it.name, it.price))

    def place_order(self, args):
        restaurant, items = parse_order_args(args)
        if not restaurant or not items:
            print('[Client] usage: order <restaurant> "<item>" <qty> ["<item>" <qty> ...]')
            return
        req = pb.OrderRequest(
            restaurant_name=restaurant,
            items=[pb.OrderItem(name=n, quantity=q) for n, q in items],
        )
        try:
            resp = self.stub.PlaceOrder(req)
        except grpc.RpcError as e:
            return rpc_error(e)
        print("[Client] Order placed successfully.")
        print("[Client] Order ID: %s" % resp.order_id)
        print("[Client] Total: %d" % resp.total)
        print("[Client] Status: %s" % STATUS_NAME(resp.status))

    def order_status(self, order_id):
        try:
            resp = self.stub.GetOrderStatus(pb.OrderStatusRequest(order_id=order_id))
        except grpc.RpcError as e:
            return rpc_error(e)
        items = ", ".join("%s x%d" % (it.name, it.quantity) for it in resp.items)
        print("[Client] Order %s : %s" % (resp.order_id, STATUS_NAME(resp.status)))
        print("[Client]   %s | %s | Total: %d" % (resp.restaurant_name, items, resp.total))

    def track_order(self, order_id):
        def worker():
            try:
                for upd in self.stub.SubscribeToOrderUpdates(pb.OrderRequest(order_id=order_id)):
                    print("\n[Update] Order %s : %s\n> " % (upd.order_id, STATUS_NAME(upd.status)),
                          end="", flush=True)
                print("\n[Client] Tracking of %s finished (order reached a final state).\n> "
                      % order_id, end="", flush=True)
            except grpc.RpcError as e:
                if e.code() != grpc.StatusCode.CANCELLED:
                    print("\n[Error] %s (%s)\n> " % (e.details(), e.code().name), end="", flush=True)

        print("[Client] Tracking order %s..." % order_id)
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        self.track_threads.append(t)

    def cancel_order(self, order_id):
        try:
            resp = self.stub.CancelOrder(pb.CancelOrderRequest(order_id=order_id))
        except grpc.RpcError as e:
            return rpc_error(e)
        print("[Client] %s" % resp.message)

    # ---- REPL --------------------------------------------------------------
    def run(self):
        print("[Client] Connected. Type 'help' for commands.")
        print(MENU)
        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            try:
                tokens = shlex.split(line)
            except ValueError as e:
                print("[Client] Could not parse command: %s" % e)
                continue
            cmd, args = tokens[0].lower(), tokens[1:]

            if cmd in ("restaurants", "1"):
                self.list_restaurants()
            elif cmd in ("order", "2"):
                self.place_order(args)
            elif cmd in ("status", "3") and len(args) == 1:
                self.order_status(args[0])
            elif cmd in ("track", "4") and len(args) == 1:
                self.track_order(args[0])
            elif cmd in ("cancel", "5") and len(args) == 1:
                self.cancel_order(args[0])
            elif cmd in ("exit", "quit", "6"):
                break
            elif cmd == "help":
                print(MENU)
            else:
                print("[Client] Unknown command. Type 'help'.")
        self.channel.close()
        print("[Client] Bye.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 customer.py <host:port>   e.g. python3 customer.py localhost:50051")
        sys.exit(1)
    CustomerClient(sys.argv[1]).run()
