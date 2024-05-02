import ipaddress

# Prompt the user for the private network
network_str = input("Please enter the private network range that you want to split (e.g., 154.0.0.0/8): ")

# Convert the input to an IPv4Network object
network = ipaddress.IPv4Network(network_str)

# Prompt the user for the new subnet prefix
new_prefix = int(input("Please enter the new subnet prefix (e.g., 25): "))

# Generate the subnets
subnets = list(network.subnets(new_prefix=new_prefix))

# Format the subnets with double quotes and commas, and enclose in square brackets
subnets_str = '[' + ', '.join(f'"{subnet}"' for subnet in subnets) + ']'

# Open a file for writing
with open('subnets.txt', 'w') as file:
    # Write the formatted subnets to the file
    file.write(subnets_str)
