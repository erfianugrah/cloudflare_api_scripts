import json
import os
import argparse
from pathlib import Path

def get_account_id(provided_id=None):
    """Get Cloudflare account ID from args, environment variable, or use default var."""
    if provided_id:
        return provided_id
    
    env_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
    if env_id:
        return env_id
        
    return "var.cloudflare_account_tag"

def clean_property_name(property_name):
    """Remove '_PropMgr' suffix from property name."""
    if property_name.endswith('_PropMgr'):
        return property_name[:-8]  # Remove last 8 characters ('_PropMgr')
    return property_name

def parse_akamai_jsons(input_dir):
    """Parse all Akamai JSON files in the directory and extract property names."""
    property_names = []
    
    # Get all .json files in the directory
    json_files = Path(input_dir).glob('*.json')
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                
            if 'propertyName' in data:
                raw_property_name = data['propertyName']
                clean_name = clean_property_name(raw_property_name)
                property_names.append(clean_name)
                print(f"Found property: {clean_name} (from {json_file.name})")
                
        except json.JSONDecodeError:
            print(f"Warning: Invalid JSON format in {json_file}")
        except Exception as e:
            print(f"Warning: Error processing {json_file}: {str(e)}")
    
    return property_names

def create_zone_tf(domain_name, account_id, zone_type="partial", output_dir='terraform', folder_name=None):
    """Create a Terraform file for a single zone."""
    # Sanitize domain name for filename
    filename = f"{domain_name.replace('.', '_')}.tf"
    
    # Create terraform template
    template = f'''resource "cloudflare_zone" "{domain_name.replace(".", "_")}" {{
  account_id = "{account_id}"
  paused     = false
  plan       = "enterprise"
  type       = "{zone_type}"
  zone       = "{domain_name}"
}}
'''
    
    # Determine the final output directory
    final_output_dir = Path(output_dir)
    if folder_name:
        final_output_dir = final_output_dir / folder_name
    
    # Create output directory if it doesn't exist
    final_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write the file
    output_path = final_output_dir / filename
    with open(output_path, 'w') as f:
        f.write(template)
    
    print(f"Created Terraform file for {domain_name} at {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Generate Cloudflare zone Terraform files from Akamai JSON configurations')
    parser.add_argument('input_dir', help='Directory containing Akamai JSON files')
    parser.add_argument('-o', '--output', default='terraform',
                      help='Output directory for Terraform files (default: terraform)')
    parser.add_argument('--folder', help='Folder name for grouping zones')
    parser.add_argument('--account-id', 
                      help='Cloudflare account ID (default: var.cloudflare_account_tag or CLOUDFLARE_ACCOUNT_ID env var)')
    parser.add_argument('--type', choices=['partial', 'full'], default='partial',
                      help='Zone type (default: partial)')
    
    args = parser.parse_args()
    
    # Get account ID with precedence: CLI arg > env var > default var
    account_id = get_account_id(args.account_id)
    
    # Parse all Akamai JSONs in the directory
    property_names = parse_akamai_jsons(args.input_dir)
    
    if not property_names:
        print("No valid property names found in the JSON files.")
        exit(1)
    
    # Create Terraform files for each property
    for domain in property_names:
        create_zone_tf(domain, account_id, args.type, args.output, args.folder)
    
    print(f"\nProcessed {len(property_names)} zones successfully!")

if __name__ == "__main__":
    main()
