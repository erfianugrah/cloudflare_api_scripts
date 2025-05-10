import subprocess
import xml.etree.ElementTree as ET
import json
import re
import sys


def run_nmap_scan(target, scan_type="basic", ports=None):
    """
    Run an Nmap scan against a target
    
    Parameters:
    - target: IP address or hostname to scan
    - scan_type: Type of scan to perform (basic, comprehensive, stealth, vuln)
    - ports: Optional port specification (e.g., "22,80,443" or "1-1000")
    
    Returns structured scan results as a dictionary
    """
    # Basic validation
    if not target or re.search(r'[;|&]', target):
        raise ValueError(f"Invalid target: {target}")
    
    # Base command
    args = ["nmap", "-oX", "-"]
    
    # Add port specification if provided
    if ports:
        args.extend(["-p", ports])
    
    # Add scan-specific options
    if scan_type == "basic":
        args.extend(["-sV"])  # Version detection
    elif scan_type == "comprehensive":
        args.extend(["-sV", "-sC", "-O"])  # Version, scripts, OS detection
    elif scan_type == "stealth":
        args.extend(["-sS", "-T2"])  # SYN scan, slower timing
    elif scan_type == "vuln":
        args.extend(["-sV", "--script=vuln"])  # Vulnerability scan
    else:
        raise ValueError(f"Invalid scan type: {scan_type}")
    
    # Add target
    args.append(target)
    
    try:
        # Run Nmap
        process = subprocess.run(args, capture_output=True, text=True, check=True)
        
        # Parse XML output
        return parse_nmap_output(process.stdout)
        
    except subprocess.CalledProcessError as e:
        print(f"Error running Nmap: {e.stderr}", file=sys.stderr)
        if "requires root privileges" in e.stderr:
            print("This scan type requires root/admin privileges", file=sys.stderr)
        return None


def parse_nmap_output(xml_output):
    """Parse Nmap XML output into a structured dictionary"""
    try:
        root = ET.fromstring(xml_output)
        result = {"hosts": []}
        
        # Extract host information
        for host in root.findall("./host"):
            host_data = {
                "status": host.find("./status").attrib.get("state", "unknown"),
                "addresses": [],
                "ports": []
            }
            
            # Get IP addresses
            for addr in host.findall("./address"):
                if addr.attrib.get("addrtype") == "ipv4":
                    host_data["addresses"].append(addr.attrib.get("addr"))
            
            # Get hostnames
            hostnames_elem = host.find("./hostnames")
            if hostnames_elem is not None:
                host_data["hostname"] = next(
                    (h.attrib.get("name") for h in hostnames_elem.findall("./hostname")), 
                    None
                )
            
            # Get ports
            ports_elem = host.find("./ports")
            if ports_elem is not None:
                for port in ports_elem.findall("./port"):
                    state_elem = port.find("./state")
                    service_elem = port.find("./service")
                    
                    # Only include open ports
                    if state_elem is not None and state_elem.attrib.get("state") == "open":
                        port_data = {
                            "protocol": port.attrib.get("protocol"),
                            "port": port.attrib.get("portid"),
                            "service": service_elem.attrib.get("name") if service_elem is not None else "unknown",
                            "product": service_elem.attrib.get("product", "") if service_elem is not None else "",
                            "version": service_elem.attrib.get("version", "") if service_elem is not None else ""
                        }
                        host_data["ports"].append(port_data)
            
            result["hosts"].append(host_data)
        
        return result
    
    except ET.ParseError:
        print("Failed to parse Nmap XML output", file=sys.stderr)
        return None


def highlight_critical_services(scan_results):
    """Analyze scan results and highlight potentially vulnerable services"""
    if not scan_results or "hosts" not in scan_results:
        return scan_results
    
    # Common vulnerable services and their risk levels
    vulnerable_services = {
        "ftp": "HIGH",
        "telnet": "CRITICAL",
        "smtp": "MEDIUM",
        "http": "MEDIUM",
        "https": "LOW",
        "smb": "HIGH",
        "microsoft-ds": "HIGH",
        "ms-sql": "HIGH",
        "mysql": "HIGH",
        "postgresql": "HIGH",
        "rdp": "HIGH"
    }
    
    # Critical ports to watch for
    critical_ports = {
        "21": "FTP",
        "23": "Telnet",
        "25": "SMTP",
        "80": "HTTP",
        "445": "SMB",
        "1433": "MS-SQL",
        "3306": "MySQL",
        "3389": "RDP",
        "5432": "PostgreSQL"
    }
    
    # Add vulnerability assessment
    scan_results["vulnerabilities"] = []
    
    # Check each host
    for host in scan_results["hosts"]:
        ip = host["addresses"][0] if host["addresses"] else "unknown"
        
        for port in host["ports"]:
            service = port["service"].lower()
            port_num = port["port"]
            
            # Check if it's a known vulnerable service
            if service in vulnerable_services:
                finding = {
                    "host": ip,
                    "port": port_num,
                    "service": service,
                    "risk": vulnerable_services[service],
                    "details": f"{service.upper()} on port {port_num} ({port['product']} {port['version']})"
                }
                scan_results["vulnerabilities"].append(finding)
                port["risk"] = vulnerable_services[service]
                
            # Check if it's a critical port number
            elif port_num in critical_ports:
                finding = {
                    "host": ip,
                    "port": port_num,
                    "service": service,
                    "risk": "MEDIUM",
                    "details": f"Port {port_num} typically used for {critical_ports[port_num]}"
                }
                scan_results["vulnerabilities"].append(finding)
                port["risk"] = "MEDIUM"
    
    return scan_results


def format_report(scan_results):
    """Format scan results into a readable text report"""
    if not scan_results:
        return "No scan results available"
    
    output = []
    output.append("NETWORK SECURITY SCAN RESULTS")
    output.append("=" * 50)
    
    # Count vulnerable services by risk
    risk_summary = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for vuln in scan_results.get("vulnerabilities", []):
        risk_summary[vuln["risk"]] = risk_summary.get(vuln["risk"], 0) + 1
    
    # Summary
    output.append(f"\nScanned hosts: {len(scan_results['hosts'])}")
    output.append(f"Potential vulnerabilities: {len(scan_results.get('vulnerabilities', []))}")
    for risk, count in risk_summary.items():
        if count > 0:
            output.append(f"  {risk} risk: {count}")
    
    # Host details
    for host in scan_results["hosts"]:
        ip = host["addresses"][0] if host["addresses"] else "unknown"
        hostname = host.get("hostname", "")
        
        output.append(f"\nHOST: {ip} {f'({hostname})' if hostname else ''}")
        output.append("-" * 50)
        
        # Open ports
        if host["ports"]:
            for port in sorted(host["ports"], key=lambda p: int(p["port"])):
                risk = port.get("risk", "")
                risk_indicator = f"[{risk}]" if risk else ""
                
                output.append(
                    f"{port['port']}/{port['protocol']:<4} {port['service']:<15} "
                    f"{port['product']} {port['version']} {risk_indicator}"
                )
        else:
            output.append("No open ports found")
    
    # Critical findings
    if scan_results.get("vulnerabilities"):
        output.append("\nPOTENTIAL SECURITY ISSUES")
        output.append("-" * 50)
        
        # Group by host
        for host in scan_results["hosts"]:
            ip = host["addresses"][0] if host["addresses"] else "unknown"
            host_vulns = [v for v in scan_results["vulnerabilities"] if v["host"] == ip]
            
            if host_vulns:
                output.append(f"\n{ip}:")
                for vuln in sorted(host_vulns, key=lambda v: v["risk"]):
                    output.append(f"  [{vuln['risk']}] {vuln['details']}")
    
    return "\n".join(output)


def main():
    """Command-line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Network Security Scanner")
    parser.add_argument("--target", required=True, help="IP address or hostname to scan")
    parser.add_argument(
        "--type", 
        choices=["basic", "comprehensive", "stealth", "vuln"], 
        default="basic",
        help="Scan type"
    )
    parser.add_argument("--ports", help="Ports to scan (e.g., '22,80,443')")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    # Run scan
    print(f"Scanning {args.target}...", file=sys.stderr)
    results = run_nmap_scan(args.target, args.type, args.ports)
    
    if results:
        # Analyze results
        enhanced_results = highlight_critical_services(results)
        
        # Output
        if args.json:
            print(json.dumps(enhanced_results, indent=2))
        else:
            print(format_report(enhanced_results))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
