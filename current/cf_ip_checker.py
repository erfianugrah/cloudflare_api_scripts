"""
CloudflareIPs - A library for working with Cloudflare IP ranges

This script provides functionality to:
1. Fetch Cloudflare IP ranges (both IPv4 and IPv6)
2. Fetch JDCloud IP ranges used by Cloudflare in China
   (see https://developers.cloudflare.com/china-network/reference/infrastructure/)
3. Check if an IP address belongs to Cloudflare (including China network)
4. Get information about all Cloudflare IP ranges
"""

import requests
import ipaddress
import logging
from datetime import datetime
import argparse
import sys

class CloudflareIPs:
    """Class to handle Cloudflare IP ranges and lookups"""
    
    # API endpoint
    CF_API_URL = "https://api.cloudflare.com/client/v4/ips"
    
    def __init__(self, include_china=True, verbose=0):
        """
        Initialize the CloudflareIPs object
        
        Args:
            include_china (bool, optional): Whether to include China (JDCloud) IPs. Defaults to True.
            verbose (int, optional): Verbosity level (0-3). Defaults to 0.
        """
        self.include_china = include_china
        self.verbose = verbose
        
        # Set up logging - using a unique logger name to avoid duplicates
        self.logger = self._setup_logger("CloudflareIPs.lib", verbose)
        
        # Regular Cloudflare IPs
        self.ipv4_ranges = []
        self.ipv6_ranges = []
        self.ipv4_cidrs = []
        self.ipv6_cidrs = []
        
        # China (JDCloud) IPs
        self.china_ipv4_ranges = []
        self.china_ipv6_ranges = []
        self.china_ipv4_cidrs = []
        self.china_ipv6_cidrs = []
        
        self.last_updated = None
        
        # Load IP ranges immediately
        self.update()
    
    def _setup_logger(self, name, verbose):
        """Set up logging based on verbosity level"""
        logger = logging.getLogger(name)
        
        # Clear any existing handlers
        if logger.handlers:
            logger.handlers = []
            
        # Set log level based on verbosity
        if verbose == 0:
            logger.setLevel(logging.WARNING)
        elif verbose == 1:
            logger.setLevel(logging.INFO)
        else:  # verbose >= 2
            logger.setLevel(logging.DEBUG)
            
        # Create a custom handler to avoid duplicate logs
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Important: prevent propagation to avoid duplicate logs
        logger.propagate = False
        
        logger.debug(f"Logging initialized with verbosity level {verbose}")
        return logger
    
    def _create_network_objects(self):
        """Create IP network objects for faster lookups"""
        self.logger.debug("Creating IP network objects...")
        
        self.ipv4_cidrs = [ipaddress.IPv4Network(cidr) for cidr in self.ipv4_ranges]
        self.ipv6_cidrs = [ipaddress.IPv6Network(cidr) for cidr in self.ipv6_ranges]
        
        self.logger.debug(f"Created {len(self.ipv4_cidrs)} IPv4 and {len(self.ipv6_cidrs)} IPv6 network objects")
        
        if self.include_china:
            self.china_ipv4_cidrs = [ipaddress.IPv4Network(cidr) for cidr in self.china_ipv4_ranges]
            self.china_ipv6_cidrs = [ipaddress.IPv6Network(cidr) for cidr in self.china_ipv6_ranges]
            
            self.logger.debug(f"Created {len(self.china_ipv4_cidrs)} China IPv4 and {len(self.china_ipv6_cidrs)} China IPv6 network objects")
    
    def update(self):
        """Update IP ranges from Cloudflare API"""
        self.logger.info("Fetching IP ranges from Cloudflare API...")
        success = True
        
        # Fetch standard Cloudflare IPs
        try:
            self.logger.debug(f"Requesting standard Cloudflare IPs from {self.CF_API_URL}")
            response = requests.get(self.CF_API_URL)
            response.raise_for_status()
            
            self.logger.debug(f"API Response status code: {response.status_code}")
            
            if self.verbose >= 2:
                self.logger.debug(f"API Response headers: {dict(response.headers)}")
                self.logger.debug(f"API Response body (partial): {response.text[:500]}...")
            
            data = response.json()
            
            if data.get('success'):
                self.ipv4_ranges = data.get('result', {}).get('ipv4_cidrs', [])
                self.ipv6_ranges = data.get('result', {}).get('ipv6_cidrs', [])
                self.last_updated = datetime.now()
                
                self.logger.info(f"Successfully fetched {len(self.ipv4_ranges)} IPv4 and {len(self.ipv6_ranges)} IPv6 ranges")
                
                if self.verbose >= 1:
                    self.logger.info(f"IPv4 ranges: {', '.join(self.ipv4_ranges[:10])}" + 
                                   (f" and {len(self.ipv4_ranges) - 10} more..." if len(self.ipv4_ranges) > 10 else ""))
                    self.logger.info(f"IPv6 ranges: {', '.join(self.ipv6_ranges[:10])}" + 
                                   (f" and {len(self.ipv6_ranges) - 10} more..." if len(self.ipv6_ranges) > 10 else ""))
                
                if self.verbose >= 2 and self.ipv4_ranges:
                    self.logger.debug(f"All IPv4 ranges: {', '.join(self.ipv4_ranges)}")
                if self.verbose >= 2 and self.ipv6_ranges:
                    self.logger.debug(f"All IPv6 ranges: {', '.join(self.ipv6_ranges)}")
            else:
                errors = data.get('errors', 'Unknown error')
                self.logger.error(f"Error fetching Cloudflare IPs: {errors}")
                success = False
                
        except requests.RequestException as e:
            self.logger.error(f"Error fetching Cloudflare IPs: {e}")
            success = False
        
        # Fetch China (JDCloud) IPs if needed
        if self.include_china:
            try:
                china_url = "https://api.cloudflare.com/client/v4/ips?networks=jdcloud"
                self.logger.debug(f"Requesting China (JDCloud) IPs from {china_url}")
                response = requests.get(china_url)
                response.raise_for_status()
                
                self.logger.debug(f"API Response status code: {response.status_code}")
                
                if self.verbose >= 2:
                    self.logger.debug(f"API Response headers: {dict(response.headers)}")
                    self.logger.debug(f"API Response body (partial): {response.text[:500]}...")
                
                data = response.json()
                
                if data.get('success'):
                    # JDCloud IPs are under the 'jdcloud_cidrs' key
                    jdcloud_cidrs = data.get('result', {}).get('jdcloud_cidrs', [])
                    
                    # Separate IPv4 and IPv6 ranges from jdcloud_cidrs
                    self.china_ipv4_ranges = []
                    self.china_ipv6_ranges = []
                    
                    for cidr in jdcloud_cidrs:
                        try:
                            ip_network = ipaddress.ip_network(cidr)
                            if ip_network.version == 4:
                                self.china_ipv4_ranges.append(cidr)
                            else:
                                self.china_ipv6_ranges.append(cidr)
                        except ValueError:
                            self.logger.warning(f"Invalid JDCloud CIDR: {cidr}")
                    
                    if self.verbose >= 2:
                        self.logger.debug(f"Raw JDCloud API response: {data}")
                    
                    self.logger.info(f"Successfully fetched {len(self.china_ipv4_ranges)} China IPv4 and {len(self.china_ipv6_ranges)} China IPv6 ranges")
                    
                    if self.verbose >= 1:
                        self.logger.info(f"China IPv4 ranges: {', '.join(self.china_ipv4_ranges[:10])}" + 
                                       (f" and {len(self.china_ipv4_ranges) - 10} more..." if len(self.china_ipv4_ranges) > 10 else ""))
                        self.logger.info(f"China IPv6 ranges: {', '.join(self.china_ipv6_ranges[:10])}" + 
                                       (f" and {len(self.china_ipv6_ranges) - 10} more..." if len(self.china_ipv6_ranges) > 10 else ""))
                    
                    if self.verbose >= 2 and self.china_ipv4_ranges:
                        self.logger.debug(f"All China IPv4 ranges: {', '.join(self.china_ipv4_ranges)}")
                    if self.verbose >= 2 and self.china_ipv6_ranges:
                        self.logger.debug(f"All China IPv6 ranges: {', '.join(self.china_ipv6_ranges)}")
                else:
                    errors = data.get('errors', 'Unknown error')
                    self.logger.error(f"Error fetching China IPs: {errors}")
                    success = False
                    
            except requests.RequestException as e:
                self.logger.error(f"Error fetching China IPs: {e}")
                success = False
        
        # Create IP network objects for faster lookups
        self._create_network_objects()
            
        return success
    
    def is_cloudflare_ip(self, ip, include_china=None):
        """
        Check if an IP address belongs to Cloudflare
        
        Args:
            ip (str): IP address to check
            include_china (bool, optional): Whether to check China IPs as well.
                                           Defaults to the class's include_china setting.
            
        Returns:
            bool: True if the IP belongs to Cloudflare, False otherwise
        """
        if include_china is None:
            include_china = self.include_china
        
        self.logger.info(f"Checking if {ip} is a Cloudflare IP (include_china={include_china})")
            
        try:
            # Convert string to IP address object
            ip_obj = ipaddress.ip_address(ip)
            ip_version = "IPv4" if ip_obj.version == 4 else "IPv6"
            self.logger.debug(f"IP {ip} parsed as {ip_version}")
            
            # Display IP ranges for verbose mode (even at level 1)
            if self.verbose >= 1:
                if ip_obj.version == 4:
                    self.logger.info(f"Standard Cloudflare IPv4 ranges to check against: " + 
                                   f"{', '.join(self.ipv4_ranges[:5])}" + 
                                   (f" and {len(self.ipv4_ranges) - 5} more..." if len(self.ipv4_ranges) > 5 else ""))
                    
                    if include_china:
                        self.logger.info(f"China IPv4 ranges to check against: " + 
                                       f"{', '.join(self.china_ipv4_ranges[:5])}" + 
                                       (f" and {len(self.china_ipv4_ranges) - 5} more..." if len(self.china_ipv4_ranges) > 5 else ""))
                else:  # IPv6
                    self.logger.info(f"Standard Cloudflare IPv6 ranges to check against: " + 
                                   f"{', '.join(self.ipv6_ranges[:5])}" + 
                                   (f" and {len(self.ipv6_ranges) - 5} more..." if len(self.ipv6_ranges) > 5 else ""))
                    
                    if include_china:
                        self.logger.info(f"China IPv6 ranges to check against: " + 
                                       f"{', '.join(self.china_ipv6_ranges[:5])}" + 
                                       (f" and {len(self.china_ipv6_ranges) - 5} more..." if len(self.china_ipv6_ranges) > 5 else ""))
            
            # Show more details in verbose mode
            if self.verbose >= 2:
                if ip_obj.version == 4:
                    self.logger.debug(f"Will check against {len(self.ipv4_cidrs)} standard Cloudflare {ip_version} ranges")
                    if include_china:
                        self.logger.debug(f"Will also check against {len(self.china_ipv4_cidrs)} China {ip_version} ranges")
                else:  # IPv6
                    self.logger.debug(f"Will check against {len(self.ipv6_cidrs)} standard Cloudflare {ip_version} ranges")
                    if include_china:
                        self.logger.debug(f"Will also check against {len(self.china_ipv6_cidrs)} China {ip_version} ranges")
                
                # In extra verbose mode, list all ranges we'll check against
                if self.verbose >= 3:
                    if ip_obj.version == 4:
                        self.logger.debug(f"All standard {ip_version} ranges to check: {self.ipv4_ranges}")
                        if include_china:
                            self.logger.debug(f"All China {ip_version} ranges to check: {self.china_ipv4_ranges}")
                    else:  # IPv6
                        self.logger.debug(f"All standard {ip_version} ranges to check: {self.ipv6_ranges}")
                        if include_china:
                            self.logger.debug(f"All China {ip_version} ranges to check: {self.china_ipv6_ranges}")
            
            # Check if IPv4 or IPv6 in standard Cloudflare ranges
            if ip_obj.version == 4:
                checked_count = 0
                total_ranges = len(self.ipv4_cidrs)
                self.logger.debug(f"Starting check against {total_ranges} standard Cloudflare IPv4 ranges")
                
                for i, network in enumerate(self.ipv4_cidrs):
                    checked_count += 1
                    if self.verbose >= 2 and checked_count % 10 == 0:
                        self.logger.debug(f"Checked {checked_count}/{total_ranges} IPv4 ranges...")
                        
                    if ip_obj in network:
                        self.logger.info(f"IP {ip} FOUND in Cloudflare IPv4 range: {self.ipv4_ranges[i]}")
                        return True
                
                self.logger.debug(f"IP {ip} NOT found in any of the {total_ranges} standard Cloudflare IPv4 ranges")
                        
                # Check China IPs if requested
                if include_china:
                    checked_count = 0
                    total_china_ranges = len(self.china_ipv4_cidrs)
                    self.logger.debug(f"Starting check against {total_china_ranges} China IPv4 ranges")
                    
                    for i, network in enumerate(self.china_ipv4_cidrs):
                        checked_count += 1
                        if self.verbose >= 2 and checked_count % 10 == 0:
                            self.logger.debug(f"Checked {checked_count}/{total_china_ranges} China IPv4 ranges...")
                            
                        if ip_obj in network:
                            self.logger.info(f"IP {ip} FOUND in Cloudflare China IPv4 range: {self.china_ipv4_ranges[i]}")
                            return True
                            
                    self.logger.debug(f"IP {ip} NOT found in any of the {total_china_ranges} China IPv4 ranges")
            else:  # IPv6
                checked_count = 0
                total_ranges = len(self.ipv6_cidrs)
                self.logger.debug(f"Starting check against {total_ranges} standard Cloudflare IPv6 ranges")
                
                for i, network in enumerate(self.ipv6_cidrs):
                    checked_count += 1
                    if self.verbose >= 2 and checked_count % 10 == 0:
                        self.logger.debug(f"Checked {checked_count}/{total_ranges} IPv6 ranges...")
                        
                    if ip_obj in network:
                        self.logger.info(f"IP {ip} FOUND in Cloudflare IPv6 range: {self.ipv6_ranges[i]}")
                        return True
                        
                self.logger.debug(f"IP {ip} NOT found in any of the {total_ranges} standard Cloudflare IPv6 ranges")
                
                # Check China IPs if requested
                if include_china:
                    checked_count = 0
                    total_china_ranges = len(self.china_ipv6_cidrs)
                    self.logger.debug(f"Starting check against {total_china_ranges} China IPv6 ranges")
                    
                    for i, network in enumerate(self.china_ipv6_cidrs):
                        checked_count += 1
                        if self.verbose >= 2 and checked_count % 10 == 0:
                            self.logger.debug(f"Checked {checked_count}/{total_china_ranges} China IPv6 ranges...")
                            
                        if ip_obj in network:
                            self.logger.info(f"IP {ip} FOUND in Cloudflare China IPv6 range: {self.china_ipv6_ranges[i]}")
                            return True
                            
                    self.logger.debug(f"IP {ip} NOT found in any of the {total_china_ranges} China IPv6 ranges")
            
            self.logger.info(f"IP {ip} is NOT found in any Cloudflare range")          
            return False
        except ValueError:
            # Invalid IP address
            self.logger.warning(f"Invalid IP address: {ip}")
            return False
            
    def is_cloudflare_china_ip(self, ip):
        """
        Check if an IP address belongs to Cloudflare's China network (JDCloud)
        
        Args:
            ip (str): IP address to check
            
        Returns:
            bool: True if the IP belongs to Cloudflare's China network, False otherwise
        """
        self.logger.info(f"Checking if {ip} is a Cloudflare China (JDCloud) IP")
        
        try:
            # Convert string to IP address object
            ip_obj = ipaddress.ip_address(ip)
            ip_version = "IPv4" if ip_obj.version == 4 else "IPv6"
            self.logger.debug(f"IP {ip} parsed as {ip_version}")
            
            # Display the China IP ranges we're checking against at verbosity level 1
            if self.verbose >= 1:
                if ip_obj.version == 4:
                    # Display all China IPv4 ranges at verbosity level 1
                    self.logger.info(f"China IPv4 ranges to check against ({len(self.china_ipv4_ranges)} total):")
                    for i, cidr in enumerate(self.china_ipv4_ranges):
                        self.logger.info(f"  {i+1}. {cidr}")
                else:  # IPv6
                    # Display all China IPv6 ranges at verbosity level 1
                    self.logger.info(f"China IPv6 ranges to check against ({len(self.china_ipv6_ranges)} total):")
                    for i, cidr in enumerate(self.china_ipv6_ranges):
                        self.logger.info(f"  {i+1}. {cidr}")
            
            # Show more details in verbose mode
            if self.verbose >= 2:
                if ip_obj.version == 4:
                    self.logger.debug(f"Will check against {len(self.china_ipv4_cidrs)} China {ip_version} ranges")
                else:  # IPv6
                    self.logger.debug(f"Will check against {len(self.china_ipv6_cidrs)} China {ip_version} ranges")
            
            # Check if IPv4 or IPv6
            if ip_obj.version == 4:
                checked_count = 0
                total_ranges = len(self.china_ipv4_cidrs)
                self.logger.debug(f"Starting check against {total_ranges} China IPv4 ranges")
                
                for i, network in enumerate(self.china_ipv4_cidrs):
                    checked_count += 1
                    if self.verbose >= 2 and checked_count % 10 == 0:
                        self.logger.debug(f"Checked {checked_count}/{total_ranges} China IPv4 ranges...")
                        
                    if ip_obj in network:
                        self.logger.info(f"IP {ip} FOUND in Cloudflare China IPv4 range: {self.china_ipv4_ranges[i]}")
                        return True
                        
                self.logger.debug(f"IP {ip} NOT found in any of the {total_ranges} China IPv4 ranges")
            else:  # IPv6
                checked_count = 0
                total_ranges = len(self.china_ipv6_cidrs)
                self.logger.debug(f"Starting check against {total_ranges} China IPv6 ranges")
                
                for i, network in enumerate(self.china_ipv6_cidrs):
                    checked_count += 1
                    if self.verbose >= 2 and checked_count % 10 == 0:
                        self.logger.debug(f"Checked {checked_count}/{total_ranges} China IPv6 ranges...")
                        
                    if ip_obj in network:
                        self.logger.info(f"IP {ip} FOUND in Cloudflare China IPv6 range: {self.china_ipv6_ranges[i]}")
                        return True
                        
                self.logger.debug(f"IP {ip} NOT found in any of the {total_ranges} China IPv6 ranges")
            
            self.logger.info(f"IP {ip} is NOT found in any Cloudflare China range")            
            return False
        except ValueError:
            # Invalid IP address
            self.logger.warning(f"Invalid IP address: {ip}")
            return False
    
    def get_ipv4_ranges(self, include_china=None):
        """
        Get the list of IPv4 ranges belonging to Cloudflare
        
        Args:
            include_china (bool, optional): Whether to include China IPs.
                                           Defaults to the class's include_china setting.
        """
        if include_china is None:
            include_china = self.include_china
        
        self.logger.debug(f"Getting IPv4 ranges (include_china={include_china})")
            
        if include_china:
            self.logger.debug(f"Returning {len(self.ipv4_ranges) + len(self.china_ipv4_ranges)} IPv4 ranges (including China)")
            return self.ipv4_ranges + self.china_ipv4_ranges
        else:
            self.logger.debug(f"Returning {len(self.ipv4_ranges)} IPv4 ranges (excluding China)")
            return self.ipv4_ranges
    
    def get_ipv6_ranges(self, include_china=None):
        """
        Get the list of IPv6 ranges belonging to Cloudflare
        
        Args:
            include_china (bool, optional): Whether to include China IPs.
                                           Defaults to the class's include_china setting.
        """
        if include_china is None:
            include_china = self.include_china
        
        self.logger.debug(f"Getting IPv6 ranges (include_china={include_china})")
            
        if include_china:
            self.logger.debug(f"Returning {len(self.ipv6_ranges) + len(self.china_ipv6_ranges)} IPv6 ranges (including China)")
            return self.ipv6_ranges + self.china_ipv6_ranges
        else:
            self.logger.debug(f"Returning {len(self.ipv6_ranges)} IPv6 ranges (excluding China)")
            return self.ipv6_ranges
    
    def get_all_ranges(self, include_china=None):
        """
        Get all IP ranges (both IPv4 and IPv6) belonging to Cloudflare
        
        Args:
            include_china (bool, optional): Whether to include China IPs.
                                           Defaults to the class's include_china setting.
        """
        if include_china is None:
            include_china = self.include_china
        
        self.logger.debug(f"Getting all IP ranges (include_china={include_china})")
            
        result = {
            'ipv4': self.ipv4_ranges,
            'ipv6': self.ipv6_ranges
        }
        
        if include_china:
            self.logger.debug("Including China IP ranges in result")
            result.update({
                'china_ipv4': self.china_ipv4_ranges,
                'china_ipv6': self.china_ipv6_ranges
            })
            
        return result
    
    def get_last_updated(self):
        """Get the timestamp when the IP ranges were last updated"""
        return self.last_updated
        
    def count_ips(self, include_china=None):
        """
        Count the total number of IPs in Cloudflare's network
        
        Args:
            include_china (bool, optional): Whether to include China IPs.
                                           Defaults to the class's include_china setting.
        
        Returns:
            dict: A dictionary with the counts for IPv4 and IPv6 addresses
        """
        if include_china is None:
            include_china = self.include_china
            
        self.logger.debug(f"Counting IPs (include_china={include_china})")
        
        ipv4_count = sum(network.num_addresses for network in self.ipv4_cidrs)
        ipv6_networks = len(self.ipv6_cidrs)
        
        self.logger.debug(f"Standard network: {ipv4_count} IPv4 addresses, {ipv6_networks} IPv6 networks")
        
        if include_china:
            china_ipv4_count = sum(network.num_addresses for network in self.china_ipv4_cidrs)
            china_ipv6_networks = len(self.china_ipv6_cidrs)
            
            self.logger.debug(f"China network: {china_ipv4_count} IPv4 addresses, {china_ipv6_networks} IPv6 networks")
            
            return {
                'ipv4_count': ipv4_count,
                'ipv6_networks': ipv6_networks,
                'china_ipv4_count': china_ipv4_count,
                'china_ipv6_networks': china_ipv6_networks,
                'total_ipv4_count': ipv4_count + china_ipv4_count,
                'total_ipv6_networks': ipv6_networks + china_ipv6_networks
            }
        else:
            return {
                'ipv4_count': ipv4_count,
                'ipv6_networks': ipv6_networks
            }


def main():
    """Main function to run from command line"""
    # Configure argument parser
    parser = argparse.ArgumentParser(description='Work with Cloudflare IP ranges')
    
    # Add global options
    parser.add_argument('-v', '--verbose', action='count', default=0, 
                        help='Increase verbosity (can be used multiple times, e.g. -vvv for extra verbose output)')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Check command
    check_parser = subparsers.add_parser('check', help='Check if an IP belongs to Cloudflare')
    check_parser.add_argument('ip', help='IP address to check')
    check_parser.add_argument('--no-china', action='store_true', help='Exclude China network from check')
    check_parser.add_argument('--china-only', action='store_true', help='Check only against China network')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List Cloudflare IP ranges')
    list_parser.add_argument('--ipv4', action='store_true', help='List only IPv4 ranges')
    list_parser.add_argument('--ipv6', action='store_true', help='List only IPv6 ranges')
    list_parser.add_argument('--no-china', action='store_true', help='Exclude China network')
    list_parser.add_argument('--china-only', action='store_true', help='List only China network IPs')
    
    # Count command
    count_parser = subparsers.add_parser('count', help='Count the number of IPs in Cloudflare\'s network')
    count_parser.add_argument('--no-china', action='store_true', help='Exclude China network')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up CLI logger (separate from the library logger)
    cli_logger = logging.getLogger("CloudflareIPs.cli")
    
    # Configure handler for CLI logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    cli_logger.addHandler(handler)
    cli_logger.propagate = False  # Prevent double logging
    
    # Set log level based on verbosity
    if args.verbose == 0:
        cli_logger.setLevel(logging.WARNING)
    elif args.verbose == 1:
        cli_logger.setLevel(logging.INFO)
    else:  # args.verbose >= 2
        cli_logger.setLevel(logging.DEBUG)
    
    cli_logger.debug(f"CLI initialized with verbosity level {args.verbose}")
    
    # Initialize CloudflareIPs
    include_china = True
    if hasattr(args, 'no_china') and args.no_china:
        include_china = False
    
    cli_logger.debug(f"Initializing CloudflareIPs (include_china={include_china}, verbose={args.verbose})")
    cf_ips = CloudflareIPs(include_china=include_china, verbose=args.verbose)
    
    if args.command == 'check':
        cli_logger.debug(f"Running 'check' command for IP: {args.ip}")
        
        if hasattr(args, 'china_only') and args.china_only:
            cli_logger.info(f"Checking if {args.ip} is a Cloudflare China (JDCloud) IP...")
            # Add information message about China Network
            if args.verbose >= 1:
                print("NOTE: Cloudflare's China Network operates through a partnership with JD Cloud")
                print("      For more information: https://developers.cloudflare.com/china-network/reference/infrastructure/")
                
            result = cf_ips.is_cloudflare_china_ip(args.ip)
            if result:
                print(f"{args.ip} is a Cloudflare China (JDCloud) IP")
            else:
                print(f"{args.ip} is NOT a Cloudflare China (JDCloud) IP")
        else:
            cli_logger.info(f"Checking if {args.ip} is a Cloudflare IP (include_china={include_china})...")
            result = cf_ips.is_cloudflare_ip(args.ip, include_china=include_china)
            if result:
                print(f"{args.ip} is a Cloudflare IP")
            else:
                print(f"{args.ip} is NOT a Cloudflare IP")
    
    elif args.command == 'list':
        cli_logger.debug(f"Running 'list' command")
        
        china_only = hasattr(args, 'china_only') and args.china_only
        cli_logger.debug(f"List options: ipv4={args.ipv4}, ipv6={args.ipv6}, china_only={china_only}")
        
        if not china_only and (args.ipv4 or not (args.ipv4 or args.ipv6)):
            print("Cloudflare IPv4 ranges:")
            for cidr in cf_ips.ipv4_ranges:
                print(cidr)
                
        if not china_only and (args.ipv6 or not (args.ipv4 or args.ipv6)):
            print("Cloudflare IPv6 ranges:")
            for cidr in cf_ips.ipv6_ranges:
                print(cidr)
        
        if include_china or china_only:
            if args.ipv4 or not (args.ipv4 or args.ipv6):
                print("\nCloudflare China (JDCloud) IPv4 ranges:")
                for cidr in cf_ips.china_ipv4_ranges:
                    print(cidr)
                    
            if args.ipv6 or not (args.ipv4 or args.ipv6):
                print("\nCloudflare China (JDCloud) IPv6 ranges:")
                for cidr in cf_ips.china_ipv6_ranges:
                    print(cidr)
                
        print(f"\nLast updated: {cf_ips.get_last_updated()}")
    
    elif args.command == 'count':
        cli_logger.debug(f"Running 'count' command")
        
        counts = cf_ips.count_ips(include_china=include_china)
        
        if include_china:
            print(f"Standard Cloudflare network: {counts['ipv4_count']} IPv4 addresses, {counts['ipv6_networks']} IPv6 networks")
            print(f"China (JDCloud) network: {counts['china_ipv4_count']} IPv4 addresses, {counts['china_ipv6_networks']} IPv6 networks")
            print(f"Total: {counts['total_ipv4_count']} IPv4 addresses, {counts['total_ipv6_networks']} IPv6 networks")
        else:
            print(f"Cloudflare has {counts['ipv4_count']} IPv4 addresses")
            print(f"Cloudflare has {counts['ipv6_networks']} IPv6 networks")
    
    else:
        cli_logger.debug("No command specified, showing help")
        parser.print_help()

if __name__ == "__main__":
    main()
