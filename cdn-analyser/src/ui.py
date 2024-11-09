from typing import List, Dict, Optional
import logging
from datetime import datetime, timezone
import sys

logger = logging.getLogger(__name__)

class UserInterface:
    def __init__(self):
        self.zones_cache = {}
    
    def select_zones(self, zones: List[Dict]) -> List[Dict]:
        """Allow user to select specific zones with sampling information."""
        try:
            print("\nAvailable Zones:")
            print("===============")
            
            for idx, zone in enumerate(zones, 1):
                print(f"{idx}. {zone['name']}")
                if 'status' in zone:
                    print(f"   Status: {zone['status']}")
                if 'development_mode' in zone:
                    print(f"   Development Mode: {'On' if zone['development_mode'] else 'Off'}")
            
            while True:
                try:
                    selection = input(
                        "\nEnter zone numbers to analyze (comma-separated, or 'all' for all zones): "
                    ).strip()
                    
                    if selection.lower() == 'all':
                        logger.info("Selected all zones for analysis")
                        return zones
                    
                    if selection.lower() == 'q':
                        print("Exiting...")
                        sys.exit(0)
                    
                    selected_indices = [int(i.strip()) for i in selection.split(',')]
                    selected_zones = [
                        zones[i-1] for i in selected_indices 
                        if 0 < i <= len(zones)
                    ]
                    
                    if not selected_zones:
                        print("No valid zones selected. Please try again.")
                        continue
                    
                    self._confirm_selection(selected_zones)
                    return selected_zones
                    
                except ValueError:
                    print("Invalid input. Please enter numbers separated by commas or 'all'.")
                except IndexError:
                    print("One or more selected numbers are out of range. Please try again.")
                
            return []
            
        except Exception as e:
            logger.error(f"Error in zone selection: {str(e)}")
            return []

    def show_analysis_progress(
        self,
        zone_name: str,
        analysis_type: str,
        progress: float,
        sampling_rate: float,
        confidence: float
    ) -> None:
        """Display analysis progress with sampling information."""
        try:
            bar_length = 50
            filled_length = int(progress * bar_length)
            bar = '=' * filled_length + '-' * (bar_length - filled_length)
            
            print(f"\rAnalyzing {zone_name} - {analysis_type}: [{bar}] {progress*100:.1f}%", end='')
            print(f" (Sampling Rate: {sampling_rate:.1f}%, Confidence: {confidence:.2f})", end='')
            
            if progress >= 1:
                print()  # New line when complete
                
        except Exception as e:
            logger.error(f"Error displaying progress: {str(e)}")

    def show_sampling_summary(self, sampling_metrics: Dict) -> None:
        """Display sampling statistics summary."""
        try:
            print("\nSampling Analysis Summary")
            print("========================")
            print(f"Average Sampling Rate: {sampling_metrics['avg_sampling_rate']:.1f}%")
            print(f"Confidence Score: {sampling_metrics['avg_confidence_score']:.3f}")
            print(f"Total Samples: {sampling_metrics['total_samples']:,}")
            print(f"Estimated Total: {sampling_metrics['estimated_total']:,}")
            
            # Add confidence level indication
            confidence = sampling_metrics['avg_confidence_score']
            if confidence >= 0.95:
                print("Confidence Level: HIGH (Results highly reliable)")
            elif confidence >= 0.85:
                print("Confidence Level: MEDIUM (Results generally reliable)")
            else:
                print("Confidence Level: LOW (Results should be used with caution)")
                
        except Exception as e:
            logger.error(f"Error displaying sampling summary: {str(e)}")

    def _confirm_selection(self, selected_zones: List[Dict]) -> bool:
        """Confirm zone selection with user."""
        print("\nSelected zones for analysis:")
        for zone in selected_zones:
            print(f"- {zone['name']}")
        
        while True:
            confirm = input("\nConfirm selection? (y/n): ").strip().lower()
            if confirm == 'y':
                return True
            elif confirm == 'n':
                return False
            else:
                print("Please enter 'y' for yes or 'n' for no.")

    def show_completion_summary(
        self,
        start_time: datetime,
        zones_analyzed: int,
        total_requests: int,
        avg_sampling_rate: float,
        avg_confidence: float
    ) -> None:
        """Display analysis completion summary."""
        try:
            duration = datetime.now(timezone.utc) - start_time
            
            print("\nAnalysis Complete!")
            print("=================")
            print(f"Duration: {duration}")
            print(f"Zones Analyzed: {zones_analyzed}")
            print(f"Total Requests Processed: {total_requests:,}")
            print(f"Average Sampling Rate: {avg_sampling_rate:.1f}%")
            print(f"Overall Confidence Score: {avg_confidence:.3f}")
            print("\nResults have been saved to the 'reports' directory.")
            
        except Exception as e:
            logger.error(f"Error displaying completion summary: {str(e)}")

    def prompt_for_report_preferences(self) -> Dict:
        """Get user preferences for report generation."""
        try:
            print("\nReport Generation Options:")
            print("1. Summary only")
            print("2. Detailed analysis")
            print("3. Full report with raw data")
            
            choice = input("Select report type [1-3]: ").strip() or "2"
            
            include_visualizations = input("Include visualizations? (y/n): ").strip().lower() == 'y'
            include_recommendations = input("Include recommendations? (y/n): ").strip().lower() == 'y'
            
            return {
                'report_type': int(choice),
                'include_visualizations': include_visualizations,
                'include_recommendations': include_recommendations
            }
            
        except Exception as e:
            logger.error(f"Error getting report preferences: {str(e)}")
            return {
                'report_type': 2,
                'include_visualizations': True,
                'include_recommendations': True
            }
