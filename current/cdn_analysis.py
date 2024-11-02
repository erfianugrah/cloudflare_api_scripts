import requests
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import seaborn as sns

class CloudflareLogAnalyzer:
    def __init__(self):
        """
        Initialize Cloudflare Log Analyzer using environment variables
        """
        self.api_endpoint = "https://api.cloudflare.com/client/v4/graphql"
        self.headers = {
            "X-Auth-Email": os.environ.get('CLOUDFLARE_EMAIL'),
            "X-Auth-Key": os.environ.get('CLOUDFLARE_API_KEY'),
            "Content-Type": "application/json",
        }
        self.account_id = os.environ.get('CLOUDFLARE_ACCOUNT_ID')
        
        # Validate credentials
        if not self.headers["X-Auth-Email"]:
            raise ValueError("Missing CLOUDFLARE_EMAIL in environment variables")
        if not self.headers["X-Auth-Key"]:
            raise ValueError("Missing CLOUDFLARE_API_KEY in environment variables")
        if not self.account_id:
            raise ValueError("Missing CLOUDFLARE_ACCOUNT_ID in environment variables")
            
        print("Auth configuration:")
        print(f"Email configured: {'Yes' if self.headers['X-Auth-Email'] else 'No'}")
        print(f"API Key configured: {'Yes' if self.headers['X-Auth-Key'] else 'No'}")
        print(f"Account ID configured: {'Yes' if self.account_id else 'No'}")

        # Set up plotting style using a built-in style
        plt.style.use('bmh')  # Using 'bmh' style which is clean and modern
        
        # Set default figure size and DPI for better readability
        plt.rcParams['figure.figsize'] = (15, 20)
        plt.rcParams['figure.dpi'] = 100
        
        # Improve font sizes
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.labelsize'] = 12
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['xtick.labelsize'] = 10
        plt.rcParams['ytick.labelsize'] = 10
        
        # Set color cycle for consistent colors
        plt.rcParams['axes.prop_cycle'] = plt.cycler(color=['#2ecc71', '#3498db', '#e74c3c', '#f1c40f', '#9b59b6', '#1abc9c'])

    def query_logs(self, hours_ago=24):
        """
        Query Cloudflare logs using GraphQL with limited fields
        """
        start_time = (datetime.now() - timedelta(hours=hours_ago)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        query = """
        query GetHttpRequests($accountTag: String!, $start: String!) {
            viewer {
                accounts(filter: {accountTag: $accountTag}) {
                    httpRequests1hGroups(
                        limit: 1000
                        filter: {
                            datetime_gt: $start
                        }
                        orderBy: [datetime_ASC]
                    ) {
                        dimensions {
                            datetime
                        }
                        sum {
                            bytes
                            cachedBytes
                            cachedRequests
                            contentTypeMap {
                                bytes
                                requests
                                edgeResponseContentTypeName
                            }
                            countryMap {
                                requests
                                clientCountryName
                            }
                            encryptedBytes
                            encryptedRequests
                            pageViews
                            requests
                            responseStatusMap {
                                requests
                                edgeResponseStatus
                            }
                            threats
                        }
                        uniq {
                            uniques
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "accountTag": self.account_id,
            "start": start_time
        }

        print(f"\nSending GraphQL query with variables: {json.dumps(variables, indent=2)}")
        
        response = requests.post(
            self.api_endpoint,
            headers=self.headers,
            json={"query": query, "variables": variables}
        )
        
        print(f"\nAPI Response Status Code: {response.status_code}")
        
        try:
            response_data = response.json()
            
            if response.status_code != 200:
                raise Exception(f"HTTP request failed with status code: {response.status_code}")
                
            if 'errors' in response_data and response_data['errors']:
                print(f"GraphQL Errors: {json.dumps(response_data['errors'], indent=2)}")
                raise Exception(f"GraphQL query failed: {response_data['errors']}")
                
            if 'data' not in response_data:
                raise Exception("Response missing 'data' field")
                
            return response_data
            
        except json.JSONDecodeError as e:
            print(f"Failed to parse response as JSON. Response text: {response.text}")
            raise Exception(f"Invalid JSON response from API: {str(e)}")

    def analyze_performance_issues(self, log_data):
        """
        Analyze logs for performance issues
        """
        try:
            groups = log_data['data']['viewer']['accounts'][0].get('httpRequests1hGroups', [])
            if not groups:
                return {
                    "traffic": [],
                    "cache": [],
                    "security": [],
                    "content": [],
                    "warning": "No log data found in the specified time period"
                }

            analysis = {
                "traffic": [],
                "cache": [],
                "security": [],
                "content": []
            }

            for group in groups:
                # Traffic Analysis
                total_requests = group['sum'].get('requests', 0)
                unique_visitors = group['uniq'].get('uniques', 0)
                
                traffic_entry = {
                    "datetime": group['dimensions']['datetime'],
                    "requests": total_requests,
                    "unique_visitors": unique_visitors,
                    "page_views": group['sum'].get('pageViews', 0),
                    "total_bytes": group['sum'].get('bytes', 0)
                }

                # Geographic Distribution
                country_data = {
                    item['clientCountryName']: item['requests']
                    for item in group['sum'].get('countryMap', [])
                }
                traffic_entry["country_distribution"] = country_data
                analysis["traffic"].append(traffic_entry)

                # Cache Analysis
                cached_reqs = group['sum'].get('cachedRequests', 0)
                if total_requests > 0:
                    cache_ratio = (cached_reqs / total_requests) * 100
                    analysis["cache"].append({
                        "datetime": group['dimensions']['datetime'],
                        "cache_hit_ratio": round(cache_ratio, 2),
                        "cached_requests": cached_reqs,
                        "cached_bytes": group['sum'].get('cachedBytes', 0)
                    })

                # Security Analysis
                analysis["security"].append({
                    "datetime": group['dimensions']['datetime'],
                    "ssl_requests": group['sum'].get('encryptedRequests', 0),
                    "ssl_bytes": group['sum'].get('encryptedBytes', 0),
                    "total_threats": group['sum'].get('threats', 0)
                })

                # Content Analysis
                content_data = group['sum'].get('contentTypeMap', [])
                analysis["content"].append({
                    "datetime": group['dimensions']['datetime'],
                    "content_types": {
                        item['edgeResponseContentTypeName']: {
                            "requests": item['requests'],
                            "bytes": item['bytes']
                        } for item in content_data
                    }
                })

            return analysis
            
        except Exception as e:
            print(f"Error during analysis: {str(e)}")
            raise

    def generate_visual_report(self, analysis_data, output_dir='reports'):
        """
        Generate visual report with matplotlib charts and summary
        """
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Convert analysis data into DataFrames
        traffic_df = pd.DataFrame(analysis_data['traffic'])
        traffic_df['datetime'] = pd.to_datetime(traffic_df['datetime'])
        
        cache_df = pd.DataFrame(analysis_data['cache'])
        cache_df['datetime'] = pd.to_datetime(cache_df['datetime'])
        
        security_df = pd.DataFrame(analysis_data['security'])
        security_df['datetime'] = pd.to_datetime(security_df['datetime'])

        # Create figure with GridSpec
        fig = plt.figure(figsize=(15, 20))
        gs = GridSpec(4, 2, figure=fig)
        fig.suptitle('CDN Performance Dashboard', fontsize=16, y=0.95)

        # 1. Traffic Overview
        ax1 = fig.add_subplot(gs[0, :])
        ax1.plot(traffic_df['datetime'], traffic_df['requests'], marker='o', label='Requests')
        ax1.plot(traffic_df['datetime'], traffic_df['unique_visitors'], marker='s', label='Unique Visitors')
        ax1.set_title('Traffic Overview')
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Count')
        ax1.legend()
        plt.xticks(rotation=45)
        ax1.grid(True)

        # 2. Cache Performance
        ax2 = fig.add_subplot(gs[1, 0])
        ax2.plot(cache_df['datetime'], cache_df['cache_hit_ratio'], marker='o', color='green')
        ax2.set_title('Cache Hit Ratio')
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Hit Ratio (%)')
        plt.xticks(rotation=45)
        ax2.grid(True)

        # 3. Geographic Distribution
        ax3 = fig.add_subplot(gs[1, 1])
        country_data = {}
        for row in traffic_df.itertuples():
            for country, requests in row.country_distribution.items():
                country_data[country] = country_data.get(country, 0) + requests
        
        top_countries = dict(sorted(country_data.items(), key=lambda x: x[1], reverse=True)[:5])
        ax3.pie(top_countries.values(), labels=top_countries.keys(), autopct='%1.1f%%')
        ax3.set_title('Top 5 Countries by Traffic')

        # 4. Security Metrics
        ax4 = fig.add_subplot(gs[2, 0])
        ax4.bar(security_df['datetime'], security_df['ssl_requests'], color='blue', alpha=0.6)
        ax4.set_title('SSL Requests Over Time')
        ax4.set_xlabel('Time')
        ax4.set_ylabel('SSL Requests')
        plt.xticks(rotation=45)

        # 5. Content Type Distribution
        ax5 = fig.add_subplot(gs[2, 1])
        content_types = {}
        for entry in analysis_data['content']:
            for content_type, data in entry['content_types'].items():
                content_types[content_type] = content_types.get(content_type, 0) + data['requests']
        
        top_content = dict(sorted(content_types.items(), key=lambda x: x[1], reverse=True)[:5])
        ax5.pie(top_content.values(), labels=top_content.keys(), autopct='%1.1f%%')
        ax5.set_title('Top 5 Content Types')

        # 6. Bandwidth Usage
        ax6 = fig.add_subplot(gs[3, :])
        bandwidth_gb = traffic_df['total_bytes'] / 1e9
        ax6.plot(traffic_df['datetime'], bandwidth_gb, marker='o', color='purple')
        ax6.set_title('Bandwidth Usage Over Time')
        ax6.set_xlabel('Time')
        ax6.set_ylabel('Bandwidth (GB)')
        plt.xticks(rotation=45)
        ax6.grid(True)

        # Adjust layout
        plt.tight_layout()

        # Generate summary statistics
        summary = {
            "overview": {
                "total_requests": traffic_df['requests'].sum(),
                "unique_visitors": traffic_df['unique_visitors'].sum(),
                "total_bandwidth_gb": round(traffic_df['total_bytes'].sum() / 1e9, 2),
                "average_cache_hit_ratio": round(cache_df['cache_hit_ratio'].mean(), 2),
            },
            "security": {
                "total_threats": security_df['total_threats'].sum(),
                "ssl_percentage": round(
                    (security_df['ssl_requests'].sum() / traffic_df['requests'].sum()) * 100, 2
                ) if traffic_df['requests'].sum() > 0 else 0,
            },
            "top_countries": top_countries,
            "top_content_types": top_content
        }

        # Save reports
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_base = os.path.join(output_dir, f'cdn_report_{timestamp}')
        
        # Save charts
        plt.savefig(f'{report_base}_charts.png', dpi=300, bbox_inches='tight')
        
        # Generate and save text summary
        summary_text = f"""
CDN Performance Summary Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Overview Statistics:
------------------
Total Requests: {summary['overview']['total_requests']:,}
Unique Visitors: {summary['overview']['unique_visitors']:,}
Total Bandwidth: {summary['overview']['total_bandwidth_gb']:.2f} GB
Average Cache Hit Ratio: {summary['overview']['average_cache_hit_ratio']:.2f}%

Security Metrics:
---------------
Total Threats: {summary['security']['total_threats']:,}
SSL Usage: {summary['security']['ssl_percentage']:.2f}%

Top 5 Countries by Traffic:
------------------------
{"".join(f"{country}: {requests:,} requests ({requests/summary['overview']['total_requests']*100:.1f}%)\n" for country, requests in top_countries.items())}

Top 5 Content Types:
-----------------
{"".join(f"{content_type}: {requests:,} requests ({requests/summary['overview']['total_requests']*100:.1f}%)\n" for content_type, requests in top_content.items())}
"""

        with open(f'{report_base}_summary.txt', 'w') as f:
            f.write(summary_text)

        print(f"\nReports generated in '{output_dir}' directory:")
        print(f"1. Charts: cdn_report_{timestamp}_charts.png")
        print(f"2. Summary: cdn_report_{timestamp}_summary.txt")

        return summary

    def generate_report(self, hours_ago=24):
        """
        Generate comprehensive performance report with visualizations
        """
        try:
            print(f"\nGenerating report for the last {hours_ago} hours...")
            
            # Get and analyze data
            log_data = self.query_logs(hours_ago)
            analysis = self.analyze_performance_issues(log_data)
            
            # Generate visual report and get summary
            summary = self.generate_visual_report(analysis)
            
            # Return complete report
            report = {
                "timestamp": datetime.now().isoformat(),
                "analysis_period": f"Last {hours_ago} hours",
                "summary": summary,
                "detailed_analysis": analysis
            }
            
            return json.dumps(report, indent=2)
            
        except Exception as e:
            print(f"Error generating report: {str(e)}")
            raise

def main():
    try:
        print("Starting CDN Analysis...")
        analyzer = CloudflareLogAnalyzer()
        report = analyzer.generate_report(hours_ago=24)
        print("\nAnalysis complete. Check the reports directory for visual and text summaries.")
    except Exception as e:
        print(f"\nError running analysis: {str(e)}")
        import traceback
        print("\nFull traceback:")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
