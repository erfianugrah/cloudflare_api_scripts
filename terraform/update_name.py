import re
from pathlib import Path
import json
import sys
import logging
import datetime
import shutil
import argparse
from typing import Dict, List, Tuple

class ResourceRenamer:
    def __init__(self, config_file=None, backup_dir=None):
        self.mappings = self.load_mappings(config_file)
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dir = backup_dir or f"terraform_backup_{self.timestamp}"
        self.changes_log = []
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        log_file = f"resource_renamer_{self.timestamp}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Started resource renaming operation - Backup directory: {self.backup_dir}")

    def load_mappings(self, config_file=None) -> dict:
        """Load resource mappings from config file or use defaults"""
        default_mappings = {
            "exact": {
                "cloudflare_device_dex_test": "cloudflare_zero_trust_dex_test",
                "cloudflare_device_settings_policy": "cloudflare_zero_trust_device_profiles",
                "cloudflare_access_keys_configuration": "cloudflare_zero_trust_key_access_key_configuration",
                "cloudflare_access_ca_certificate": "cloudflare_zero_trust_access_short_lived_certificate",
                "cloudflare_fallback_domain": "cloudflare_zero_trust_local_fallback_domain",
                "cloudflare_teams_account": "cloudflare_zero_trust_gateway_settings",
                "cloudflare_teams_location": "cloudflare_zero_trust_dns_location",
                "cloudflare_teams_rule": "cloudflare_zero_trust_gateway_policy"
            },
            "patterns": {
                "cloudflare_teams_": "cloudflare_zero_trust_gateway_",
                "cloudflare_": "cloudflare_zero_trust_"
            }
        }

        if config_file:
            try:
                with open(config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading config file: {e}")
                self.logger.info("Using default mappings instead")
        return default_mappings

    def create_backup(self, file_path: Path) -> Path:
        """Create a backup of the file before modifications"""
        backup_path = Path(self.backup_dir) / file_path.relative_to(Path('.'))
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, backup_path)
        self.logger.info(f"Created backup: {backup_path}")
        return backup_path

    def generate_diff(self, original: str, modified: str) -> List[Tuple[int, str, str]]:
        """Generate a diff between original and modified content"""
        diff = []
        for i, (old, new) in enumerate(zip(original.splitlines(), modified.splitlines())):
            if old != new:
                diff.append((i + 1, old, new))
        return diff

    def transform_content(self, content: str) -> str:
        """Transform resource names based on provided mappings"""
        modified_content = content

        # First handle exact matches
        for old_name, new_name in self.mappings["exact"].items():
            pattern = f'(^|[^\\w])(?<!var\\.)({old_name})([^"\\w]*)'
            modified_content = re.sub(pattern, f'\\1{new_name}\\3', modified_content)
        
        # Then handle pattern replacements
        for old_pattern, new_pattern in self.mappings["patterns"].items():
            # Build pattern that excludes exact matches
            skip_patterns = '|'.join(re.escape(key) for key in self.mappings["exact"].keys())
            pattern = f'(?<!var\\.)(?<!zero_trust_)({old_pattern})'
            if skip_patterns:
                pattern = f'(?!({skip_patterns})){pattern}'
            
            pattern += '([^"\\s]+)'
            
            def replace_pattern(match):
                return f'{new_pattern}{match.group(2)}'
            
            modified_content = re.sub(pattern, replace_pattern, modified_content)
        
        return modified_content

    def process_file(self, file_path: Path, dry_run: bool = False) -> bool:
        """Process a single terraform file and update resource references"""
        try:
            # Read original content
            with open(file_path, 'r') as f:
                original_content = f.read()

            # Transform content
            modified_content = self.transform_content(original_content)

            # If there are changes, generate diff and get confirmation
            if original_content != modified_content:
                diff = self.generate_diff(original_content, modified_content)
                self.logger.info(f"\nProposed changes for {file_path}:")
                for line_num, old, new in diff:
                    self.logger.info(f"Line {line_num}:")
                    self.logger.info(f"  - {old}")
                    self.logger.info(f"  + {new}")
                
                if not dry_run:
                    # Create backup and store changes for potential revert
                    backup_path = self.create_backup(file_path)
                    self.changes_log.append({
                        'file': str(file_path),
                        'backup': str(backup_path),
                        'diff': diff
                    })

                    # Write changes
                    with open(file_path, 'w') as f:
                        f.write(modified_content)
                    self.logger.info(f"Updated: {file_path}")
                return True
            
            self.logger.info(f"No changes needed: {file_path}")
            return False

        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {e}")
            return False

    def revert_changes(self):
        """Revert all changes using backups"""
        self.logger.info("\nReverting changes...")
        for change in self.changes_log:
            try:
                shutil.copy2(change['backup'], change['file'])
                self.logger.info(f"Reverted: {change['file']}")
            except Exception as e:
                self.logger.error(f"Error reverting {change['file']}: {e}")

    def save_changes_log(self):
        """Save the changes log to a JSON file"""
        log_file = f"changes_log_{self.timestamp}.json"
        try:
            with open(log_file, 'w') as f:
                json.dump(self.changes_log, f, indent=2)
            self.logger.info(f"Changes log saved to: {log_file}")
        except Exception as e:
            self.logger.error(f"Error saving changes log: {e}")

def main():
    parser = argparse.ArgumentParser(description='Terraform Resource Renamer')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--backup-dir', help='Custom backup directory')
    parser.add_argument('--revert', action='store_true', help='Revert previous changes')
    parser.add_argument('--dry-run', action='store_true', help='Show changes without applying them')
    parser.add_argument('--no-confirm', action='store_true', help='Skip confirmation prompts')
    args = parser.parse_args()

    # Initialize renamer
    renamer = ResourceRenamer(args.config, args.backup_dir)

    # Files to skip
    excluded_files = {
        'variables.tf',
        'vars.tf',
        'outputs.tf',
        'terraform.tf',
        'versions.tf',
        'provider.tf'
    }

    # Find all .tf files
    terraform_files = list(Path('.').rglob('*.tf'))
    
    # Show summary before proceeding
    eligible_files = [f for f in terraform_files if f.name not in excluded_files]
    renamer.logger.info(f"\nFound {len(eligible_files)} eligible Terraform files")
    
    if not args.dry_run and not args.no_confirm:
        confirm = input("Proceed with renaming? (y/N): ").lower()
        if confirm != 'y':
            renamer.logger.info("Operation cancelled")
            return

    # Process files
    files_processed = 0
    files_changed = 0
    
    for tf_file in eligible_files:
        if tf_file.name not in excluded_files:
            if renamer.process_file(tf_file, args.dry_run):
                files_changed += 1
            files_processed += 1

    # Save changes log
    if files_changed > 0 and not args.dry_run:
        renamer.save_changes_log()

    # Summary
    renamer.logger.info(f"\nOperation Summary:")
    renamer.logger.info(f"Files processed: {files_processed}")
    renamer.logger.info(f"Files changed: {files_changed}")
    renamer.logger.info(f"Backup directory: {renamer.backup_dir}")
    renamer.logger.info(f"Log file: resource_renamer_{renamer.timestamp}.log")

    # Offer to revert
    if files_changed > 0 and not args.dry_run and not args.no_confirm:
        confirm = input("\nWould you like to revert the changes? (y/N): ").lower()
        if confirm == 'y':
            renamer.revert_changes()

if __name__ == "__main__":
    main()
