import ipaddress

# Define the /8 network
network = ipaddress.IPv4Network('154.0.0.0/8')

# Generate /25 subnets
subnets = list(network.subnets(new_prefix=25))

# Format the subnets with double quotes and commas, and enclose in square brackets
subnets_str = '[' + ', '.join(f'"{subnet}"' for subnet in subnets) + ']'

# Open a file for writing
with open('subnets.txt', 'w') as file:
    # Write the formatted subnets to the file
    file.write(subnets_str)