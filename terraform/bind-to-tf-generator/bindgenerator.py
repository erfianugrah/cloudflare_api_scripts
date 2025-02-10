import re
import sys
import argparse
import ipaddress
from typing import List, Dict, Tuple
from dataclasses import dataclass

class DNSValidationError(Exception):
    """Custom exception for DNS validation errors"""
    pass

@dataclass
class ValidationOptions:
    """Options for controlling DNS record validation"""
    skip_hostname_validation: bool = False
    skip_content_validation: bool = False
    skip_ttl_validation: bool = False
    allow_unknown_record_types: bool = False
    default_ttl: int = 1
    force_proceed: bool = False
    skip_empty_content: bool = False
    no_proxy: bool = False
    keep_apex_ns: bool = False  # Option to keep NS records at apex

def validate_hostname(hostname: str, options: ValidationOptions) -> bool:
    """
    Validate hostname according to DNS standards:
    - Max 253 characters total length
    - Each label max 63 characters
    - Labels can contain a-z, 0-9, underscores, and hyphens
    - First label can be a wildcard (*)
    - Labels separated by dots
    """
    if options.skip_hostname_validation:
        return True
        
    if not hostname or hostname == '@':  # Empty hostname or @ is valid (root domain)
        return True
        
    if len(hostname) > 253:
        raise DNSValidationError(f"Hostname '{hostname}' exceeds 253 characters")
        
    labels = hostname.split('.')
    
    for i, label in enumerate(labels):
        if not label:
            continue
            
        if i == 0 and label == '*':  # Allow wildcard as first label
            continue
            
        if len(label) > 63:
            raise DNSValidationError(f"Label '{label}' exceeds 63 characters")
            
        if label.startswith('-') or label.endswith('-'):
            raise DNSValidationError(f"Label '{label}' cannot start or end with a hyphen")
            
        if not re.match(r'^[a-zA-Z0-9_]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$', label):
            raise DNSValidationError(f"Label '{label}' contains invalid characters (only a-z, 0-9, underscore, and hyphen are allowed)")
            
    return True

def validate_record_type(record_type: str, options: ValidationOptions) -> bool:
    """Validate DNS record type"""
    valid_types = {
        'A',      # IPv4 address
        'AAAA',   # IPv6 address
        'CAA',    # Certificate Authority Authorization
        'CNAME',  # Canonical name
        'DNSKEY', # DNS Key
        'DS',     # Delegation Signer
        'HTTPS',  # HTTPS routing
        'LOC',    # Location
        'MX',     # Mail Exchange
        'NAPTR',  # Name Authority Pointer
        'NS',     # Name Server
        'PTR',    # Pointer
        'SMIMEA', # S/MIME cert association
        'SRV',    # Service locator
        'SSHFP',  # SSH Public Key Fingerprint
        'SVCB',   # Service Binding
        'TXT',    # Text
        'TLSA',   # TLSA certificate association
        'URI'     # Uniform Resource Identifier
    }
    
    if options.allow_unknown_record_types:
        return True
        
    if record_type not in valid_types:
        raise DNSValidationError(f"Invalid record type: {record_type}. Valid types are: {', '.join(sorted(valid_types))}")
    return True

def validate_ip_address(ip: str, record_type: str, options: ValidationOptions) -> bool:
    """Validate IP address based on record type (A or AAAA)"""
    if options.skip_content_validation:
        return True
        
    try:
        if record_type == 'A':
            ipaddress.IPv4Address(ip)
        elif record_type == 'AAAA':
            ipaddress.IPv6Address(ip)
        else:
            return True
    except ValueError as e:
        raise DNSValidationError(f"Invalid IP address for {record_type} record: {ip}")
    return True

def validate_ttl(ttl: str, options: ValidationOptions) -> bool:
    """Validate TTL value"""
    if options.skip_ttl_validation:
        return True
        
    try:
        ttl_val = int(ttl)
        if ttl_val < 0 or ttl_val > 2147483647:  # Max 32-bit signed integer
            raise DNSValidationError(f"Invalid TTL value: {ttl}. Must be between 0 and 2147483647")
    except ValueError:
        raise DNSValidationError(f"TTL must be a number: {ttl}")
    return True

def validate_txt_content(content: str, options: ValidationOptions) -> bool:
    """Validate TXT record content"""
    if options.skip_content_validation:
        return True
    
    if len(content) > 2048:
        raise DNSValidationError(f"TXT record content exceeds 2048 characters: {len(content)} characters")
    
    return True

def validate_srv_name(name: str) -> bool:
    """Validate SRV record name format (_service._proto.name)"""
    parts = name.split('.')
    if len(parts) < 2:  # Need at least _service and _proto
        raise DNSValidationError(f"Invalid SRV record name format '{name}'. Expected: _service._proto.name")
        
    service, proto = parts[:2]
    if not (service.startswith('_') and proto.startswith('_')):
        raise DNSValidationError(f"Invalid SRV record name format '{name}'. Service and protocol must start with underscore")
    
    return True

def extract_metadata_from_comments(line: str) -> Tuple[List[str], bool]:
    """Extract tags and proxied status from bind file comments."""
    tags = []  # Start with empty tags list
    proxied = False  # Default to not proxied
    
    if ';' not in line:
        tags = ["managed-by-terraform"]  # Only add default if no comments exist
        return tags, proxied
        
    comment = line.split(';', 1)[1].strip()
    
    # Extract cf_tags
    if 'cf_tags=' in comment:
        tag_part = comment.split('cf_tags=')[1].split(';')[0].strip()
        custom_tags = [t.strip() for t in tag_part.split(',')]
        tags.extend(t for t in custom_tags if t != 'cf-proxied:false' and t != 'cf-proxied:true')
    
    # Only add managed-by-terraform if no other tags were found
    if not tags:
        tags = ["managed-by-terraform"]
    
    # Extract proxied status
    if 'cf-proxied:false' in comment:
        proxied = False
    elif 'cf-proxied:true' in comment:
        proxied = True
        
    return tags, proxied

def process_hostname(name: str, zone_name: str, options: ValidationOptions) -> str:
    """Process hostname based on zone handling options"""
    if not name or name == '@':
        return '@'
        
    name = name.rstrip('.')
    
    # Special handling for DKIM, DMARC, and other service records
    if '_domainkey.' in name or name.startswith('_'):
        parts = name.split(f'.{zone_name}')
        return parts[0]
        
    # If the name exactly matches the zone, it's a root record
    if name == zone_name:
        return '@'
        
    # Handle wildcard records
    if name.startswith('*.'):
        if name[2:] == zone_name:
            return '*'
        if name[2:].endswith(f".{zone_name}"):
            subdomain = name[2:-len(zone_name)-1].rstrip('.')
            return f'*.{subdomain}' if subdomain else '*'
        return name[2:]
        
    # If name ends with zone_name, strip it
    if name.endswith(f".{zone_name}"):
        subdomain = name[:-len(zone_name)-1].rstrip('.')
        if not subdomain:
            return '@'
        return subdomain
            
    return name

def filter_and_deduplicate_records(records: List[Dict[str, str]], zone_name: str, options: ValidationOptions) -> List[Dict[str, str]]:
    """Filter SOA/NS records and remove duplicates."""
    seen = set()
    filtered = []
    
    for record in records:
        # Skip SOA records entirely
        if record['type'] == 'SOA':
            continue
            
        # Skip only apex NS records unless override is set
        if record['type'] == 'NS' and record['name'] == '@' and not options.keep_apex_ns:
            continue
            
        # Create unique key for deduplication
        key = (record['name'], record['type'], record['content'])
        if key not in seen:
            seen.add(key)
            filtered.append(record)
            
    return filtered

def parse_bind_zone(zone_file: str, zone_name: str, options: ValidationOptions) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Parse a BIND zone file and extract DNS records with validation."""
    valid_records = []
    invalid_records = []
    
    record_pattern = re.compile(
        r'^(?:(?P<name>[^\s]+)\s+)?(?:(?P<ttl>\d+)\s+)?(?:IN\s+)?'
        r'(?P<type>[A-Z]+)\s+'
        r'(?P<content>(?:"[^"]*"|[^;])+?)(?:\s*;.*)?$'
    )
    
    with open(zone_file, 'r') as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            
            if not line or line.startswith(';') or line.startswith('$'):
                continue
                
            match = record_pattern.match(line)
            if match:
                try:
                    record = match.groupdict()
                    record['line_number'] = line_number
                    record['original_line'] = line
                    
                    # Extract metadata from comments
                    tags, proxied = extract_metadata_from_comments(line)
                    record['tags'] = tags
                    record['proxied'] = proxied
                    
                    # Skip empty content if option is set
                    if options.skip_empty_content and not record['content'].strip():
                        continue
                    
                    # Validate record type
                    validate_record_type(record['type'], options)
                    
                    # Process hostname with zone handling
                    record['name'] = process_hostname(record.get('name', ''), zone_name, options)
                    if record['type'] == 'SRV' and not options.skip_hostname_validation:
                        validate_srv_name(record['name'])
                    validate_hostname(record['name'], options)
                    
                    # Set and validate TTL
                    record['ttl'] = record['ttl'] or str(options.default_ttl)
                    validate_ttl(record['ttl'], options)
                    
                    # Clean up and validate content based on record type
                    content = record['content'].strip()
                    if record['type'] in ['A', 'AAAA']:
                        content = content.strip('" ')
                        validate_ip_address(content, record['type'], options)
                    elif record['type'] == 'MX':
                        content = content.strip('" ')
                    elif record['type'] in ['CNAME', 'NS', 'PTR']:
                        content = content.strip('" ')
                        validate_hostname(content.rstrip('.'), options)
                    elif record['type'] == 'TXT':
                        # Special handling for DKIM records
                        if '_domainkey' in record['name']:
                            # Remove any existing quotes and join multiple parts
                            content = content.replace('"', '').strip()
                            content = f'"{content}"'
                        else:
                            # For regular TXT records, preserve existing quotes
                            if content.startswith('"') and content.endswith('"'):
                                content = content
                            else:
                                content = f'"{content}"'
                        validate_txt_content(content, options)
                        
                    record['content'] = content
                    valid_records.append(record)
                    
                except DNSValidationError as e:
                    if options.force_proceed:
                        valid_records.append(record)
                        print(f"Warning: Proceeding with invalid record at line {line_number} despite error: {str(e)}")
                    else:
                        record['error'] = str(e)
                        invalid_records.append(record)
                    continue
                
    return valid_records, invalid_records

def generate_record_block(record: Dict[str, str], record_name: str) -> str:
    """Generate Terraform block for a single DNS record."""
    block = [f'resource "cloudflare_record" "{record_name}" {{']
    
    # Common attributes
    block.extend([
        f'  zone_id = cloudflare_zone.zone.id',
        f'  name    = "{record["name"]}"',
        f'  type    = "{record["type"]}"',
        f'  ttl     = {record["ttl"]}'
    ])
    
    # Handle different record types
    if record['type'] == 'SRV':
        parts = record['content'].strip().split()
        block.extend([
            '  data {',
            f'    priority = {parts[0]}',
            f'    weight   = {parts[1]}',
            f'    port     = {parts[2]}',
            f'    target   = "{parts[3].rstrip(".")}"',
            '  }',
            '  proxied = false'
        ])
    elif record['type'] == 'MX':
        priority, target = record['content'].strip().split(maxsplit=1)
        block.extend([
            f'  priority = {priority}',
            f'  content  = "{target.rstrip(".")}"',
            '  proxied  = false'
        ])
    elif record['type'] == 'CAA':
        flags, tag, value = record['content'].strip().split(maxsplit=2)
        block.extend([
            '  data {',
            f'    flags = {flags}',
            f'    tag   = "{tag}"',
            f'    value = {value}',
            '  }',
            '  proxied = false'
        ])
    else:
        # A, AAAA, CNAME, TXT, etc.
        content = record['content']
        if record['type'] != 'TXT' and not content.startswith('"'):
            content = f'"{content.rstrip(".")}"'
        block.append(f'  content = {content}')
        
        # Handle proxying
        can_be_proxied = record['type'] in ['A', 'CNAME']
        is_proxied = record.get('proxied', can_be_proxied and not record.get('no_proxy', False))
        block.append(f'  proxied = {str(is_proxied).lower()}')
    
    # Add tags if present
    if record.get('tags'):
        tags = ', '.join(f'"{tag}"' for tag in record['tags'])
        block.append(f'  tags = [{tags}]')
    
    block.append('}')
    return '\n'.join(block)

def generate_terraform(valid_records: List[Dict[str, str]], invalid_records: List[Dict[str, str]], 
                      options: ValidationOptions, zone_name: str) -> str:
    """Generate Terraform configuration for Cloudflare DNS records."""
    terraform_config = []
    
    # Add warning header for invalid records
    if invalid_records:
        terraform_config.append('# WARNING: The following records had validation errors and were skipped:')
        for record in invalid_records:
            terraform_config.append(f'# Line {record["line_number"]}: {record["original_line"]}')
            terraform_config.append(f'#   Error: {record["error"]}')
        terraform_config.append('#\n')
    
    # Filter and deduplicate records
    filtered_records = filter_and_deduplicate_records(valid_records, zone_name, options)
    
    # Generate record blocks
    for i, record in enumerate(filtered_records, 1):
        terraform_config.append(generate_record_block(record, f"record_{i}"))
    
    return "\n\n".join(terraform_config)

def main():
    parser = argparse.ArgumentParser(
        description='Convert BIND zone file to Cloudflare Terraform configuration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Basic usage with domain example.com
  %(prog)s zone.file example.com
  
  # Using subdomain as zone (e.g., for records under www.example.com)
  %(prog)s zone.file www.example.com
  
  # Wildcard record example (*.example.com)
  %(prog)s zone.file example.com
  
  # SRV record example (_sip._tcp.example.com)
  %(prog)s zone.file example.com --skip-hostname-validation
  
  # Keep NS records at apex
  %(prog)s zone.file example.com --keep-apex-ns
  
  # Disable proxying for all records
  %(prog)s zone.file example.com --no-proxy
  
  # Custom output file and TTL
  %(prog)s zone.file example.com -o custom-dns.tf --default-ttl 3600
''')
    
    parser.add_argument('zone_file', help='Path to the BIND zone file')
    parser.add_argument('zone_name', help='Zone name (e.g., example.com or sub.example.com)')
    
    # Validation control options
    parser.add_argument('--skip-hostname-validation', action='store_true', help='Skip hostname validation')
    parser.add_argument('--skip-content-validation', action='store_true', help='Skip content validation')
    parser.add_argument('--skip-ttl-validation', action='store_true', help='Skip TTL validation')
    parser.add_argument('--allow-unknown-types', action='store_true', help='Allow unknown record types')
    parser.add_argument('--force-proceed', action='store_true', help='Proceed with invalid records')
    parser.add_argument('--skip-empty-content', action='store_true', help='Skip records with empty content')
    parser.add_argument('--keep-apex-ns', action='store_true', help='Keep NS records at zone apex')
    
    # Default value options
    parser.add_argument('--default-ttl', type=int, default=1, help='Default TTL value (default: 1)')
    parser.add_argument('--no-proxy', action='store_true', 
                       help='Disable proxying for all records (by default, only A and CNAME records are proxied)')
    
    # Output options
    parser.add_argument('--output', '-o', default='dns.tf', help='Output file path (default: dns.tf)')
    
    args = parser.parse_args()
    
    # Create validation options from arguments
    options = ValidationOptions(
        skip_hostname_validation=args.skip_hostname_validation,
        skip_content_validation=args.skip_content_validation,
        skip_ttl_validation=args.skip_ttl_validation,
        allow_unknown_record_types=args.allow_unknown_types,
        default_ttl=args.default_ttl,
        no_proxy=args.no_proxy,
        force_proceed=args.force_proceed,
        skip_empty_content=args.skip_empty_content,
        keep_apex_ns=args.keep_apex_ns
    )
    
    try:
        valid_records, invalid_records = parse_bind_zone(args.zone_file, args.zone_name, options)
        
        if not valid_records and not invalid_records:
            print("Error: No DNS records found in the input file")
            sys.exit(1)
        
        if invalid_records and not options.force_proceed:
            print("\nWarning: Found invalid DNS records:")
            for record in invalid_records:
                print(f"Line {record['line_number']}: {record['original_line']}")
                print(f"  Error: {record['error']}")
            print("\nUse --force-proceed to include invalid records anyway")
            print("Use --skip-*-validation options to bypass specific validations")
            print()
        
        terraform_config = generate_terraform(valid_records, invalid_records, options, args.zone_name)
        
        with open(args.output, 'w') as f:
            f.write(terraform_config)
        
        print(f"Successfully converted {len(valid_records)} DNS records to Terraform format")
        if invalid_records:
            print(f"Skipped {len(invalid_records)} invalid records")
        print(f"Output written to {args.output}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
