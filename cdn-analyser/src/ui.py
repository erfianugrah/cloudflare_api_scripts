from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class UserInterface:
    def select_zones(self, zones: List[Dict]) -> List[Dict]:
        """Allow user to select specific zones to analyze."""
        print("\nAvailable zones:")
        for idx, zone in enumerate(zones, 1):
            print(f"{idx}. {zone['name']} ({zone['id']})")
        
        while True:
            try:
                selection = input("\nEnter zone numbers to analyze (comma-separated, or 'all' for all zones): ").strip()
                if selection.lower() == 'all':
                    logger.info("Selected all zones for analysis")
                    return zones
                
                selected_indices = [int(i.strip()) for i in selection.split(',')]
                selected_zones = [zones[i-1] for i in selected_indices if 0 < i <= len(zones)]
                
                if selected_zones:
                    logger.info(f"Selected {len(selected_zones)} zones for analysis")
                    return selected_zones
                
                print("No valid zones selected. Please try again.")
                
            except (ValueError, IndexError):
                print("Invalid input. Please enter numbers separated by commas or 'all'.")
            except Exception as e:
                logger.error(f"Error during zone selection: {str(e)}")
                print("An error occurred during selection. Please try again.")
