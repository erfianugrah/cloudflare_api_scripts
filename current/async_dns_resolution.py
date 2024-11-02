import socket
import json
import argparse
import time
from typing import Dict, List, Union, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
from pathlib import Path
import csv
import sys
from queue import Queue
from itertools import islice

class FastDnsResolver:
    def __init__(self, workers: int = 50, timeout: float = 2.0, batch_size: int = 100):
        """
        Initialize DNS resolver with optimized settings.
        
        Args:
            workers: Number of worker threads
            timeout: DNS query timeout in seconds
            batch_size: Number of domains to process in each batch
        """
        self.workers = workers
        self.timeout = timeout
        self.batch_size = batch_size
        self.results: Dict[str, Union[str, List[str]]] = {}
        self.results_lock = Lock()
        self.processed_count = 0
        self.total_domains = 0

    def _resolve_domain(self, domain: str) -> tuple:
        """Resolve a single domain."""
        try:
            socket.setdefaulttimeout(self.timeout)
            ips = socket.gethostbyname_ex(domain)[2]
            return domain, ips if len(ips) > 1 else ips[0]
        except (socket.gaierror, socket.timeout) as e:
            return domain, f"Error: {str(e)}"

    def _process_batch(self, batch: List[str]) -> None:
        """Process a batch of domains."""
        for domain in batch:
            domain, result = self._resolve_domain(domain)
            with self.results_lock:
                self.results[domain] = result
                self.processed_count += 1
                
                # Print progress
                progress = (self.processed_count / self.total_domains) * 100
                domains_per_sec = self.processed_count / (time.time() - self.start_time)
                print(f"\rProgress: {progress:.1f}% ({self.processed_count}/{self.total_domains}) "
                      f"- {domains_per_sec:.1f} domains/sec", end="", flush=True)

    def resolve_domains(self, domains: List[str]) -> Dict:
        """
        Resolve domains using batched processing with thread pool.
        """
        self.start_time = time.time()
        self.total_domains = len(domains)
        self.processed_count = 0
        
        print(f"Starting resolution of {self.total_domains} domains using {self.workers} threads")
        
        # Process domains in batches using thread pool
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = []
            
            # Submit batches to thread pool
            for i in range(0, len(domains), self.batch_size):
                batch = domains[i:i + self.batch_size]
                futures.append(executor.submit(self._process_batch, batch))
            
            # Wait for all batches to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"\nError processing batch: {e}", file=sys.stderr)

        # Calculate statistics
        end_time = time.time()
        resolution_time = round(end_time - self.start_time, 3)
        successful_resolutions = sum(1 for result in self.results.values() 
                                  if not isinstance(result, str) or not result.startswith("Error"))
        
        print("\nResolution completed!")
        
        return {
            "metadata": {
                "resolution_time": resolution_time,
                "domains_processed": self.total_domains,
                "domains_resolved": successful_resolutions,
                "success_rate": round((successful_resolutions / self.total_domains) * 100, 2),
                "average_time_per_domain": round(resolution_time / self.total_domains, 3),
                "domains_per_second": round(self.total_domains / resolution_time, 2)
            },
            "results": self.results
        }

def read_domains(file_path: str) -> List[str]:
    """Efficiently read domains from a file."""
    domains: Set[str] = set()
    path = Path(file_path)
    
    try:
        if path.suffix.lower() == '.csv':
            with path.open() as f:
                # Simple CSV processing
                reader = csv.reader(f)
                # Try to detect header
                first_row = next(reader)
                if not any(cell.lower().endswith('.com') or cell.lower().endswith('.net') 
                          or cell.lower().endswith('.org') for cell in first_row):
                    # Looks like a header, start from next row
                    domains.add(first_row[0].strip().lower())
                
                # Process remaining rows
                for row in reader:
                    if row:
                        domain = row[0].strip().lower()
                        if domain and '.' in domain:
                            domains.add(domain)
        else:
            # Fast processing for text files
            with path.open() as f:
                for line in f:
                    domain = line.strip().lower()
                    if domain and not domain.startswith('#') and '.' in domain:
                        domains.add(domain)
                
        return sorted(domains)  # Return sorted list for consistent processing
    
    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Fast DNS Resolver (Standard Library Version)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -f domains.txt -o results.json
  %(prog)s -f domains.csv -w 100 -b 200 -t 1.5
  
File Formats:
  - CSV: First column should contain domains
  - TXT: One domain per line, lines starting with # are ignored
""")
    
    parser.add_argument('-f', '--file', required=True,
                       help='Input file (CSV or TXT) containing domains')
    parser.add_argument('-w', '--workers', type=int, default=50,
                       help='Number of worker threads (default: 50)')
    parser.add_argument('-t', '--timeout', type=float, default=2.0,
                       help='DNS query timeout in seconds (default: 2.0)')
    parser.add_argument('-b', '--batch-size', type=int, default=100,
                       help='Batch size for processing (default: 100)')
    parser.add_argument('-o', '--output',
                       help='Output file path (default: stdout)')
    
    args = parser.parse_args()
    
    try:
        print(f"Reading domains from {args.file}...")
        domains = read_domains(args.file)
        print(f"Loaded {len(domains)} unique domains")
        
        resolver = FastDnsResolver(
            workers=args.workers,
            timeout=args.timeout,
            batch_size=args.batch_size
        )
        
        results = resolver.resolve_domains(domains)
        
        # Output results
        json_output = json.dumps(results, indent=2)
        if args.output:
            Path(args.output).write_text(json_output)
            print(f"Results written to {args.output}")
        else:
            print(json_output)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
