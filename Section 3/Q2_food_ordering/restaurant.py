#!/usr/bin/env python3
"""restaurant.py -- Restaurant CLI for the Food Ordering system.

    python3 restaurant.py localhost:50051 "Pizza House"

Commands
  pending                 view orders for this restaurant that are not finished
  accept  <order_id>      PLACED    -> ACCEPTED
  prepare <order_id>      ACCEPTED  -> PREPARING
  ready   <order_id>      PREPARING -> READY
  help
  exit
"""
import sys

import grpc

import food_ordering_pb2 as pb
import food_ordering_pb2_grpc as pb_grpc

STATUS_NAME = pb.OrderStatus.Name

MENU = """
Commands:
  1. pending               View Pending Orders
  2. accept  <order_id>    Accept Order
  3. prepare <order_id>    Start Preparing
  4. ready   <order_id>    Mark Ready
  5. exit
"""


def rpc_error(e):
    print("[Error] %s (%s)" % (e.details(), e.code().name))


class RestaurantClient:
    def __init__(self, address, name):
        self.name = name
        self.channel = grpc.insecure_channel(address)
        self.stub = pb_grpc.FoodOrderingServiceStub(self.channel)

    def pending(self):
        try:
            resp = self.stub.GetPendingOrders(pb.PendingOrdersRequest(restaurant_name=self.name))
        except grpc.RpcError as e:
            return rpc_error(e)
        if not resp.orders:
            print("[Restaurant] No pending orders.")
            return
        for o in resp.orders:
            items = ", ".join("%s x%d" % (it.name, it.quantity) for it in o.items)
            print("[Restaurant] Order %s : %s   (%s | Total: %d)"
                  % (o.order_id, STATUS_NAME(o.status), items, o.total))

    def update(self, order_id, new_status):
        req = pb.OrderStatusUpdate(order_id=order_id, restaurant_name=self.name, new_status=new_status)
        try:
            resp = self.stub.UpdateOrderStatus(req)
        except grpc.RpcError as e:
            return rpc_error(e)
        print("[Restaurant] %s" % resp.message)

    def run(self):
        print("[Restaurant] Logged in as '%s'. Type 'help' for commands." % self.name)
        print(MENU)
        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            parts = line.split()
            cmd, args = parts[0].lower(), parts[1:]

            if cmd in ("pending", "1"):
                self.pending()
            elif cmd in ("accept", "2") and len(args) == 1:
                self.update(args[0], pb.ACCEPTED)
            elif cmd in ("prepare", "3") and len(args) == 1:
                self.update(args[0], pb.PREPARING)
            elif cmd in ("ready", "4") and len(args) == 1:
                self.update(args[0], pb.READY)
            elif cmd in ("exit", "quit", "5"):
                break
            elif cmd == "help":
                print(MENU)
            else:
                print("[Restaurant] Unknown command. Type 'help'.")
        self.channel.close()
        print("[Restaurant] Bye.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('usage: python3 restaurant.py <host:port> "<Restaurant Name>"')
        print('  e.g. python3 restaurant.py localhost:50051 "Pizza House"')
        sys.exit(1)
    RestaurantClient(sys.argv[1], sys.argv[2]).run()
