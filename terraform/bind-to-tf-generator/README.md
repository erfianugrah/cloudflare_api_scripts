# Cloudflare DNS Management Tools

This toolset helps manage Cloudflare DNS records using Terraform. It consists of two main scripts:
- `executor.py`: A simple wrapper to list zones and handle DNS exports
- `bindgenerator.py`: Converts BIND zone files to Terraform configurations

## Prerequisites

- Python 3.6+
- Required Python packages:
  ```bash
  pip install requests argparse
  ```
- Cloudflare API credentials (either API Token or API Key + Email)
- Terraform installed

## Authentication

Set your Cloudflare credentials using environment variables:
```bash
# Using API Token (recommended)
export CLOUDFLARE_API_TOKEN="your_token"

# Or using API Key + Email
export CLOUDFLARE_API_KEY="your_key"
export CLOUDFLARE_EMAIL="your_email"
```

## Using the Tools

### Basic Workflow

1. List available zones:
```bash
python executor.py --list-zones
```

2. Export DNS records for a zone:
```bash
python executor.py --export-dns ZONE_ID
```

3. Convert to Terraform configuration:
```bash
python executor.py zone_ZONE_ID.bind example.com
```

### Advanced Usage with bindgenerator.py

The bindgenerator supports various flags for customization:

```bash
# Custom TTL and proxy settings
python bindgenerator.py zone.file example.com --default-ttl 3600 --no-proxy

# Skip validations
python bindgenerator.py zone.file example.com --skip-hostname-validation

# Keep NS records at apex
python bindgenerator.py zone.file example.com --keep-apex-ns

# Custom output file
python bindgenerator.py zone.file example.com -o custom-dns.tf
```

### Available Options

#### executor.py
- `--list-zones`: List all zones in your account
- `--export-dns ZONE_ID`: Export DNS records for specified zone
- Any additional arguments are passed to bindgenerator.py

#### bindgenerator.py
- `--skip-hostname-validation`: Skip hostname validation
- `--skip-content-validation`: Skip content validation
- `--skip-ttl-validation`: Skip TTL validation
- `--allow-unknown-types`: Allow unknown record types
- `--force-proceed`: Process invalid records anyway
- `--skip-empty-content`: Skip records with empty content
- `--keep-apex-ns`: Keep NS records at zone apex
- `--default-ttl`: Set default TTL value (default: 1)
- `--no-proxy`: Disable proxying for all records
- `--output, -o`: Output file path (default: dns.tf)

## Examples

### Export and Convert in One Step
```bash
# Export zone and convert to Terraform with custom TTL
python executor.py --export-dns ZONE_ID zone_ZONE_ID.bind example.com --default-ttl 3600

# Export and disable proxying
python executor.py --export-dns ZONE_ID zone_ZONE_ID.bind example.com --no-proxy
```

### Process Existing BIND File
```bash
# Convert existing BIND file
python bindgenerator.py your_zone.bind example.com

# With custom settings
python bindgenerator.py your_zone.bind example.com --default-ttl 60 --no-proxy
```

## Record Type Support

The bindgenerator supports common DNS record types:
- A/AAAA records
- CNAME records
- TXT records
- MX records
- SRV records
- CAA records
- And more...

## Safety Features

- Validates DNS record formats
- Preserves SOA records
- Option to keep NS records at apex
- Backup capabilities
- Validation options for different record types

## Common Issues

1. Authentication errors:
   - Ensure environment variables are set correctly
   - Check API token/key permissions

2. Validation errors:
   - Use `--force-proceed` to skip validation
   - Or use specific skip flags (e.g., `--skip-hostname-validation`)

3. Rate limiting:
   - The tools include basic rate limiting protection
   - For large zones, operations might take longer

## Common Use Case: Migrating DNS to Terraform

### Scenario
You have existing DNS records in Cloudflare that you want to manage with Terraform. This involves:
1. Exporting current records
2. Converting to Terraform format
3. Deleting existing records
4. Applying via Terraform

### Step-by-Step Workflow

1. **Set up authentication**:
```bash
export CLOUDFLARE_API_TOKEN="your_token"
# or
export CLOUDFLARE_API_KEY="your_key"
export CLOUDFLARE_EMAIL="your_email"
```

2. **List available zones** (optional):
```bash
python executor.py --list-zones
```

3. **Export current DNS records and generate Terraform config**:
```bash
# Export DNS records to BIND format and convert to Terraform
python executor.py --export-dns YOUR_ZONE_ID zone_YOUR_ZONE_ID.bind example.com

# The above command will:
# - Export DNS records to zone_YOUR_ZONE_ID.bind
# - Generate dns.tf with the Terraform configuration
```

4. **Delete existing DNS records**:
```bash
# Review and delete existing records
python delete_dns_records.py --zone-ids YOUR_ZONE_ID

# The script will:
# - Show the number of records found
# - Ask for confirmation before deletion
```

5. **Apply Terraform configuration**:
```bash
# Initialize Terraform if needed
terraform init

# Apply the configuration
terraform apply
```

### Important Notes
- Always backup your DNS records before starting
- Lower TTL values before making changes
- The delete operation cannot be undone
- Terraform will manage the records going forward
- Test on a non-production domain first if possible

## Additional Notes

- The generated Terraform configuration uses the Cloudflare provider
- Records can be proxied or unproxied using the `--no-proxy` flag
- TTL settings can be customized using `--default-ttl`
- The tools support tags and comments in DNS records
