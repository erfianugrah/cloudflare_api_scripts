import ipaddress

prefix = '168.86.128.0/18'
network = ipaddress.ip_network(prefix)

first_ip = network.network_address
last_ip = network.broadcast_address

print("First IP:", first_ip)
print("Last IP:", last_ip)