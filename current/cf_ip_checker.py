#!/usr/bin/env python3
"""
CloudflareIPs - Tool for checking and managing Cloudflare IP ranges

This script provides functionality to:
1. Fetch and merge Cloudflare IP ranges from all endpoints (including all China colos)
2. Check if an IP address belongs to Cloudflare's network
3. List and count IP ranges from different Cloudflare networks
4. Compare IP ranges between different China colocation options
"""

import requests
import ipaddress
import argparse
import sys
import json
import os
from datetime import datetime

class CloudflareIPs:
    """Class to handle Cloudflare IP ranges and IP checking"""
    
    # API endpoints
    CF_API_URL = "https://api.cloudflare.com/client/v4/ips"
    
    def __init__(self, verbose=False, china_colos=None, ip_file=None):
        """
        Initialize with options for China colocation and verbosity
        
        Args:
            verbose (bool): Whether to print verbose information
            china_colos (list): List of China colos to query (jdc, 1, 2)
            ip_file (str): Path to a JSON file with IP ranges (for offline use)
        """
        self.verbose = verbose
        self.china_colos = china_colos or ["jdc", "1", "2"]  # Default to all colos
        self.ip_file = ip_file
        
        # Initialize IP range arrays
        self.ipv4_ranges = []
        self.ipv6_ranges = []
        self.china_ipv4_ranges = []
        self.china_ipv6_ranges = []
        
        # Network objects for faster lookup
        self.ipv4_networks = []
        self.ipv6_networks = []
        self.china_ipv4_networks = []
        self.china_ipv6_networks = []
        
        self.last_updated = None
        
        # Load immediately
        self.update()
    
    def _log(self, message, level=0):
        """Simple logging with verbosity levels"""
        if self.verbose or level == 0:
            print(message)
    
    def update(self):
        """Fetch and update all IP ranges"""
        self._log(f"Fetching Cloudflare IP ranges...", level=1)
        
        # If using a file, load from there instead of API
        if self.ip_file and os.path.exists(self.ip_file):
            self._log(f"Loading IP ranges from file: {self.ip_file}", level=1)
            try:
                with open(self.ip_file, 'r') as f:
                    data = json.load(f)
                
                self.ipv4_ranges = data.get('ipv4_cidrs', [])
                self.ipv6_ranges = data.get('ipv6_cidrs', [])
                self.china_ipv4_ranges = data.get('china_ipv4_cidrs', [])
                self.china_ipv6_ranges = data.get('china_ipv6_cidrs', [])
                self.last_updated = data.get('last_updated', datetime.now().isoformat())
                
                self._log(f"Loaded {len(self.ipv4_ranges)} IPv4 and {len(self.ipv6_ranges)} IPv6 ranges", level=1)
                self._log(f"Loaded {len(self.china_ipv4_ranges)} China IPv4 and {len(self.china_ipv6_ranges)} China IPv6 ranges", level=1)
                
                # Create network objects
                self._create_network_objects()
                return True
            except Exception as e:
                self._log(f"Error loading from file: {e}")
                return False
        
        # Fetch standard Cloudflare IPs
        try:
            self._log(f"Fetching standard IPs from {self.CF_API_URL}", level=1)
            response = requests.get(self.CF_API_URL)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('success'):
                self.ipv4_ranges = data.get('result', {}).get('ipv4_cidrs', [])
                self.ipv6_ranges = data.get('result', {}).get('ipv6_cidrs', [])
                self.last_updated = datetime.now().isoformat()
                
                self._log(f"Fetched {len(self.ipv4_ranges)} standard IPv4 and {len(self.ipv6_ranges)} standard IPv6 ranges", level=1)
            else:
                self._log(f"Error fetching standard IPs: {data.get('errors', 'Unknown error')}")
                return False
                
        except Exception as e:
            self._log(f"Error fetching standard IPs: {e}")
            return False
        
        # Fetch China IPs from all specified colos
        if self.china_colos:
            china_ipv4_set = set()  # Use sets to automatically deduplicate
            china_ipv6_set = set()
            
            for colo in self.china_colos:
                try:
                    china_api_url = f"{self.CF_API_URL}?china_colo={colo}"
                    self._log(f"Fetching China IPs (colo={colo}) from {china_api_url}", level=1)
                    
                    response = requests.get(china_api_url)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    if data.get('success'):
                        jdcloud_cidrs = data.get('result', {}).get('jdcloud_cidrs', [])
                        
                        # Count new ranges for this colo
                        new_ipv4 = 0
                        new_ipv6 = 0
                        
                        # Parse and add to appropriate sets
                        for cidr in jdcloud_cidrs:
                            try:
                                ip_network = ipaddress.ip_network(cidr)
                                if ip_network.version == 4:
                                    if cidr not in china_ipv4_set:
                                        china_ipv4_set.add(cidr)
                                        new_ipv4 += 1
                                else:
                                    if cidr not in china_ipv6_set:
                                        china_ipv6_set.add(cidr)
                                        new_ipv6 += 1
                            except ValueError:
                                self._log(f"Invalid CIDR: {cidr}", level=1)
                        
                        self._log(f"Colo {colo}: Added {new_ipv4} new IPv4 and {new_ipv6} new IPv6 ranges", level=1)
                    else:
                        self._log(f"Warning fetching China IPs (colo={colo}): {data.get('errors', 'Unknown error')}", level=1)
                        
                except Exception as e:
                    self._log(f"Warning fetching China IPs (colo={colo}): {e}", level=1)
            
            # Convert sets to sorted lists
            self.china_ipv4_ranges = sorted(list(china_ipv4_set))
            self.china_ipv6_ranges = sorted(list(china_ipv6_set))
            
            self._log(f"Fetched a total of {len(self.china_ipv4_ranges)} unique China IPv4 and "
                      f"{len(self.china_ipv6_ranges)} unique China IPv6 ranges from {len(self.china_colos)} colos", level=1)
        
        # Create network objects for lookups
        self._create_network_objects()
        
        # Save to file if requested
        if self.ip_file:
            try:
                data = {
                    'ipv4_cidrs': self.ipv4_ranges,
                    'ipv6_cidrs': self.ipv6_ranges,
                    'china_ipv4_cidrs': self.china_ipv4_ranges,
                    'china_ipv6_cidrs': self.china_ipv6_ranges,
                    'last_updated': self.last_updated
                }
                
                with open(self.ip_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                self._log(f"Saved IP ranges to {self.ip_file}", level=1)
            except Exception as e:
                self._log(f"Error saving to file: {e}")
        
        return True
    
    def _create_network_objects(self):
        """Create IP network objects for faster lookups"""
        self.ipv4_networks = [ipaddress.IPv4Network(cidr) for cidr in self.ipv4_ranges]
        self.ipv6_networks = [ipaddress.IPv6Network(cidr) for cidr in self.ipv6_ranges]
        self.china_ipv4_networks = [ipaddress.IPv4Network(cidr) for cidr in self.china_ipv4_ranges]
        self.china_ipv6_networks = [ipaddress.IPv6Network(cidr) for cidr in self.china_ipv6_ranges]
    
    def is_cloudflare_ip(self, ip, include_china=True):
        """
        Check if an IP belongs to Cloudflare
        
        Args:
            ip (str): IP address to check
            include_china (bool): Whether to include China networks
            
        Returns:
            tuple: (is_cf_ip, is_china_ip, matching_range)
        """
        try:
            ip_obj = ipaddress.ip_address(ip)
            is_china = False
            matching_range = None
            
            # Check China networks first if included
            if include_china:
                if ip_obj.version == 4:
                    for network in self.china_ipv4_networks:
                        if ip_obj in network:
                            is_china = True
                            matching_range = str(network)
                            return True, True, matching_range
                else:  # IPv6
                    for network in self.china_ipv6_networks:
                        if ip_obj in network:
                            is_china = True
                            matching_range = str(network)
                            return True, True, matching_range
            
            # Check standard Cloudflare networks
            if ip_obj.version == 4:
                for network in self.ipv4_networks:
                    if ip_obj in network:
                        matching_range = str(network)
                        return True, False, matching_range
            else:  # IPv6
                for network in self.ipv6_networks:
                    if ip_obj in network:
                        matching_range = str(network)
                        return True, False, matching_range
            
            return False, False, None
            
        except ValueError:
            self._log(f"Invalid IP address: {ip}")
            return False, False, None
    
    def get_ip_counts(self):
        """Get counts of all IP ranges"""
        ipv4_count = sum(network.num_addresses for network in self.ipv4_networks)
        ipv6_count = len(self.ipv6_networks)  # IPv6 is too large to count addresses
        
        china_ipv4_count = sum(network.num_addresses for network in self.china_ipv4_networks) 
        china_ipv6_count = len(self.china_ipv6_networks)
        
        return {
            'ipv4_count': ipv4_count,
            'ipv6_count': ipv6_count,
            'china_ipv4_count': china_ipv4_count,
            'china_ipv6_count': china_ipv6_count,
            'total_ipv4_count': ipv4_count + china_ipv4_count,
            'total_ipv6_count': ipv6_count + china_ipv6_count
        }
    
    def save_all_ranges_to_file(self, filename):
        """Save all ranges to a JSON file"""
        try:
            data = {
                'ipv4_cidrs': self.ipv4_ranges,
                'ipv6_cidrs': self.ipv6_ranges,
                'china_ipv4_cidrs': self.china_ipv4_ranges,
                'china_ipv6_cidrs': self.china_ipv6_ranges,
                'last_updated': self.last_updated
            }
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
        except Exception as e:
            self._log(f"Error saving to file: {e}")
            return False

def compare_china_colos():
    """Compare the different China colo IP ranges"""
    # Create separate instances for each colo for comparison
    all_colos = ["jdc", "1", "2"] 
    colo_instances = {}
    combined_ipv4 = set()
    combined_ipv6 = set()
    
    print("\nComparing Cloudflare China IPs from different colo options...\n")
    
    # Create instances for each colo
    for colo in all_colos:
        print(f"Fetching IPs for colo={colo}...")
        colo_instances[colo] = CloudflareIPs(verbose=False, china_colos=[colo])
        
        # Add to combined sets
        combined_ipv4.update(colo_instances[colo].china_ipv4_ranges)
        combined_ipv6.update(colo_instances[colo].china_ipv6_ranges)
    
    # Print comparison table
    print("\nCloudflare China Colo IP Range Comparison:\n")
    print(f"{'Colo':<6} | {'IPv4 Ranges':<12} | {'IPv6 Ranges':<12}")
    print("-" * 35)
    
    for colo in all_colos:
        ipv4_count = len(colo_instances[colo].china_ipv4_ranges)
        ipv6_count = len(colo_instances[colo].china_ipv6_ranges)
        print(f"{colo:<6} | {ipv4_count:<12} | {ipv6_count:<12}")
    
    # Print combined stats
    print("-" * 35)
    print(f"{'TOTAL':<6} | {len(combined_ipv4):<12} | {len(combined_ipv6):<12}")
    
    # Compare IP sets
    print("\nIP Range Overlap Analysis:\n")
    
    # IPv4 Analysis
    print("IPv4 Range Overlap:")
    for i, colo1 in enumerate(all_colos):
        for j, colo2 in enumerate(all_colos):
            if i >= j:
                continue  # Skip comparing a colo with itself or duplicating comparisons
            
            colo1_ips = set(colo_instances[colo1].china_ipv4_ranges)
            colo2_ips = set(colo_instances[colo2].china_ipv4_ranges)
            
            common = colo1_ips.intersection(colo2_ips)
            only_in_colo1 = colo1_ips - colo2_ips
            only_in_colo2 = colo2_ips - colo1_ips
            
            print(f"  {colo1} vs {colo2}:")
            print(f"    Common: {len(common)} ranges")
            print(f"    Only in {colo1}: {len(only_in_colo1)} ranges")
            print(f"    Only in {colo2}: {len(only_in_colo2)} ranges")
    
    # IPv6 Analysis
    print("\nIPv6 Range Overlap:")
    for i, colo1 in enumerate(all_colos):
        for j, colo2 in enumerate(all_colos):
            if i >= j:
                continue  # Skip comparing a colo with itself or duplicating comparisons
            
            colo1_ips = set(colo_instances[colo1].china_ipv6_ranges)
            colo2_ips = set(colo_instances[colo2].china_ipv6_ranges)
            
            common = colo1_ips.intersection(colo2_ips)
            only_in_colo1 = colo1_ips - colo2_ips
            only_in_colo2 = colo2_ips - colo1_ips
            
            print(f"  {colo1} vs {colo2}:")
            print(f"    Common: {len(common)} ranges")
            print(f"    Only in {colo1}: {len(only_in_colo1)} ranges")
            print(f"    Only in {colo2}: {len(only_in_colo2)} ranges")
    
    return

def check_ip(ip, no_china=False, verbose=False, china_colos=None, ip_file=None):
    """Check if an IP belongs to Cloudflare"""
    cf = CloudflareIPs(verbose=verbose, china_colos=china_colos, ip_file=ip_file)
    
    is_cf, is_china, matching_range = cf.is_cloudflare_ip(ip, include_china=not no_china)
    
    if is_cf:
        if is_china:
            print(f"{ip} is a Cloudflare China IP")
            if matching_range:
                print(f"Matching range: {matching_range}")
        else:
            print(f"{ip} is a Cloudflare IP")
            if matching_range:
                print(f"Matching range: {matching_range}")
    else:
        print(f"{ip} is NOT a Cloudflare IP")
    
    return is_cf

def list_ranges(only_ipv4=False, only_ipv6=False, no_china=False, only_china=False, 
               verbose=False, china_colos=None, ip_file=None):
    """List IP ranges from Cloudflare"""
    cf = CloudflareIPs(verbose=verbose, china_colos=china_colos, ip_file=ip_file)
    
    if not only_china and (only_ipv4 or not (only_ipv4 or only_ipv6)):
        print("Cloudflare IPv4 ranges:")
        for cidr in cf.ipv4_ranges:
            print(f"  {cidr}")
    
    if not only_china and (only_ipv6 or not (only_ipv4 or only_ipv6)):
        print("\nCloudflare IPv6 ranges:")
        for cidr in cf.ipv6_ranges:
            print(f"  {cidr}")
    
    if not no_china and (only_ipv4 or not (only_ipv4 or only_ipv6)):
        print("\nCloudflare China IPv4 ranges:")
        for cidr in cf.china_ipv4_ranges:
            print(f"  {cidr}")
    
    if not no_china and (only_ipv6 or not (only_ipv4 or only_ipv6)):
        print("\nCloudflare China IPv6 ranges:")
        for cidr in cf.china_ipv6_ranges:
            print(f"  {cidr}")
    
    print(f"\nLast updated: {cf.last_updated}")
    
    return

def count_ips(no_china=False, verbose=False, china_colos=None, ip_file=None):
    """Count the number of IPs in the Cloudflare network"""
    cf = CloudflareIPs(verbose=verbose, china_colos=china_colos, ip_file=ip_file)
    counts = cf.get_ip_counts()
    
    print("\nCloudflare IP Counts:\n")
    
    print(f"Standard Cloudflare network:")
    print(f"  IPv4: {counts['ipv4_count']:,} addresses in {len(cf.ipv4_ranges)} ranges")
    print(f"  IPv6: {counts['ipv6_count']} networks")
    
    if not no_china:
        print(f"\nChina (JDCloud) network:")
        print(f"  IPv4: {counts['china_ipv4_count']:,} addresses in {len(cf.china_ipv4_ranges)} ranges")
        print(f"  IPv6: {counts['china_ipv6_count']} networks")
        
        print(f"\nTotal (Standard + China):")
        print(f"  IPv4: {counts['total_ipv4_count']:,} addresses")
        print(f"  IPv6: {counts['total_ipv6_count']} networks")

def main():
    """Main entry point for the command-line tool"""
    parser = argparse.ArgumentParser(description='Cloudflare IP Ranges Tool')
    
    # Global options
    parser.add_argument('-v', '--verbose', action='store_true',
                      help='Verbose output')
    parser.add_argument('--china-colos', type=str, default="jdc,1,2",
                      help='Comma-separated list of China colos to fetch. Options: jdc,1,2. Default: all')
    parser.add_argument('--ip-file', type=str, 
                      help='Load/save IP data from/to this JSON file')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Check command
    check_parser = subparsers.add_parser('check', help='Check if an IP belongs to Cloudflare')
    check_parser.add_argument('ip', help='IP address to check')
    check_parser.add_argument('--no-china', action='store_true', 
                            help='Exclude China network from check')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List Cloudflare IP ranges')
    list_parser.add_argument('--ipv4', action='store_true', 
                           help='List only IPv4 ranges')
    list_parser.add_argument('--ipv6', action='store_true', 
                           help='List only IPv6 ranges')
    list_parser.add_argument('--no-china', action='store_true', 
                           help='Exclude China network')
    list_parser.add_argument('--china-only', action='store_true', 
                           help='List only China network IPs')
    
    # Count command
    count_parser = subparsers.add_parser('count', help='Count the number of IPs in Cloudflare\'s network')
    count_parser.add_argument('--no-china', action='store_true', 
                            help='Exclude China network')
    
    # Compare command
    subparsers.add_parser('compare', help='Compare IP ranges from different China colos')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Convert china_colos string to list
    china_colos = [colo.strip() for colo in args.china_colos.split(',')]
    
    if args.command == 'check':
        check_ip(args.ip, no_china=args.no_china, verbose=args.verbose, 
                china_colos=china_colos, ip_file=args.ip_file)
        
    elif args.command == 'list':
        list_ranges(only_ipv4=args.ipv4, only_ipv6=args.ipv6, 
                  no_china=args.no_china, only_china=args.china_only, 
                  verbose=args.verbose, china_colos=china_colos, ip_file=args.ip_file)
        
    elif args.command == 'count':
        count_ips(no_china=args.no_china, verbose=args.verbose, 
                china_colos=china_colos, ip_file=args.ip_file)
        
    elif args.command == 'compare':
        compare_china_colos()
        
    else:
        parser.print_help()

if __name__ == "__main__":
    main()