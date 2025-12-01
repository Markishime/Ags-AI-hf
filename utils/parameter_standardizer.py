"""
Parameter Standardization Utility
Handles consistent naming and mapping of soil and leaf analysis parameters
across all modules in the AGS-AI system.
"""

import re
from typing import Dict, List, Any, Optional

class ParameterStandardizer:
    """Centralized parameter standardization for soil and leaf analysis data"""
    
    def __init__(self):
        # Standard parameter names (canonical format)
        self.STANDARD_SOIL_PARAMS = {
            'pH': 'pH',
            'nitrogen': 'N (%)',
            'organic_carbon': 'Org. C (%)',
            'total_phosphorus': 'Total P (mg/kg)',
            'available_phosphorus': 'Avail P (mg/kg)',
            'exchangeable_potassium': 'Exch. K (meq/100 g)',
            'exchangeable_calcium': 'Exch. Ca (meq/100 g)',
            'exchangeable_magnesium': 'Exch. Mg (meq/100 g)',
            'cec': 'CEC (meq/100 g)'
        }
        
        self.STANDARD_LEAF_PARAMS = {
            'nitrogen': 'N (%)',
            'phosphorus': 'P (%)',
            'potassium': 'K (%)',
            'magnesium': 'Mg (%)',
            'calcium': 'Ca (%)',
            'boron': 'B (mg/kg)',
            'copper': 'Cu (mg/kg)',
            'zinc': 'Zn (mg/kg)'
        }
        
        # Comprehensive mapping of all possible parameter name variations
        self.PARAMETER_VARIATIONS = {
            # pH variations
            'pH': ['ph', 'pH', 'PH', 'p.h.', 'acidity', 'alkalinity'],
            
            # Nitrogen variations
            'N (%)': [
                'nitrogen', 'n', 'n%', 'n (%)', 'n_%', 'nitrogen%', 'nitrogen (%)', 
                'nitrogen_%', 'total n', 'total nitrogen', 'total n (%)', 'total nitrogen (%)',
                'Nitrogen (%)', 'Total Nitrogen (%)'
            ],
            
            # Organic Carbon variations
            'Org. C (%)': [
                'organic carbon', 'organic_carbon', 'carbon', 'c', 'c%', 'c (%)', 
                'c_%', 'organic_carbon_%', 'org. c (%)', 'org c (%)', 'org.c (%)',
                'Organic Carbon (%)', 'Org. C (%)'
            ],
            
            # Total Phosphorus variations
            'Total P (mg/kg)': [
                'total phosphorus', 'total p', 'total_p', 'total phosphorus mg/kg', 
                'total_p_mg_kg', 'total p (mg/kg)', 'total_p_(mg/kg)'
            ],
            
            # Available Phosphorus variations
            'Avail P (mg/kg)': [
                'available phosphorus', 'available p', 'available_p', 'avail p', 
                'avail_p', 'available phosphorus mg/kg', 'available_p_mg_kg',
                'avail p (mg/kg)', 'avail_p_(mg/kg)', 'available p (mg/kg)',
                'Available P (mg/kg)', 'Available Phosphorus (mg/kg)'
            ],
            
            # Exchangeable Potassium variations
            'Exch. K (meq/100 g)': [
                'exchangeable potassium', 'exch k', 'exch_k', 'exchangeable k',
                'exchangeable_k', 'k meq%', 'k_meq%', 'exchangeable_k_meq%',
                'exch. k (meq%)', 'exch k (meq%)', 'exch. k meq%', 'exch k meq%',
                'exch k (cmol/kg)', 'exch. k (cmol/kg)', 'exch k cmol/kg', 'exch. k cmol/kg',
                'k meq/100 g', 'k_meq/100 g', 'exchangeable_k_meq/100 g',
                'exch. k (meq/100 g)', 'exch k (meq/100 g)', 'exch. k meq/100 g', 'exch k meq/100 g'
            ],

            # Exchangeable Calcium variations
            'Exch. Ca (meq/100 g)': [
                'exchangeable calcium', 'exch ca', 'exch_ca', 'exchangeable ca',
                'exchangeable_ca', 'ca meq%', 'ca_meq%', 'exchangeable_ca_meq%',
                'exch. ca (meq%)', 'exch ca (meq%)', 'exch. ca meq%', 'exch ca meq%',
                'exch ca (cmol/kg)', 'exch. ca (cmol/kg)', 'exch ca cmol/kg', 'exch. ca cmol/kg',
                'ca meq/100 g', 'ca_meq/100 g', 'exchangeable_ca_meq/100 g',
                'exch. ca (meq/100 g)', 'exch ca (meq/100 g)', 'exch. ca meq/100 g', 'exch ca meq/100 g'
            ],

            # Exchangeable Magnesium variations
            'Exch. Mg (meq/100 g)': [
                'exchangeable magnesium', 'exch mg', 'exch_mg', 'exchangeable mg',
                'exchangeable_mg', 'mg meq%', 'mg_meq%', 'exchangeable_mg_meq%',
                'exch. mg (meq%)', 'exch mg (meq%)', 'exch. mg meq%', 'exch mg meq%',
                'exch mg (cmol/kg)', 'exch. mg (cmol/kg)', 'exch mg cmol/kg', 'exch. mg cmol/kg',
                'mg meq/100 g', 'mg_meq/100 g', 'exchangeable_mg_meq/100 g',
                'exch. mg (meq/100 g)', 'exch mg (meq/100 g)', 'exch. mg meq/100 g', 'exch mg meq/100 g'
            ],

            # CEC variations
            'CEC (meq/100 g)': [
                'cec', 'cation exchange capacity', 'c.e.c', 'cec meq%', 'cec_meq%',
                'cec (meq%)', 'c.e.c (meq%)', 'c.e.c meq%', 'cec (cmol/kg)', 'cec cmol/kg',
                'C.E.C (meq%)', 'CEC (meq%)', 'cec meq/100 g', 'cec_meq/100 g',
                'cec (meq/100 g)', 'c.e.c (meq/100 g)', 'c.e.c meq/100 g',
                'C.E.C (meq/100 g)', 'CEC (meq/100 g)'
            ],
            
            # Leaf parameter variations
            'P (%)': [
                'phosphorus', 'p', 'p%', 'p (%)', 'p_%', 'phosphorus%', 'phosphorus (%)',
                'leaf phosphorus', 'leaf p', 'leaf_p'
            ],
            
            'K (%)': [
                'potassium', 'k', 'k%', 'k (%)', 'k_%', 'potassium%', 'potassium (%)',
                'leaf potassium', 'leaf k', 'leaf_k'
            ],
            
            'Mg (%)': [
                'magnesium', 'mg', 'mg%', 'mg (%)', 'mg_%', 'magnesium%', 'magnesium (%)',
                'leaf magnesium', 'leaf mg', 'leaf_mg'
            ],
            
            'Ca (%)': [
                'calcium', 'ca', 'ca%', 'ca (%)', 'ca_%', 'calcium%', 'calcium (%)',
                'leaf calcium', 'leaf ca', 'leaf_ca'
            ],
            
            'B (mg/kg)': [
                'boron', 'b', 'b mg/kg', 'b_mg_kg', 'boron mg/kg', 'boron_mg_kg',
                'b (mg/kg)', 'boron (mg/kg)', 'leaf boron', 'leaf b', 'leaf_b'
            ],
            
            'Cu (mg/kg)': [
                'copper', 'cu', 'cu mg/kg', 'cu_mg_kg', 'copper mg/kg', 'copper_mg_kg',
                'cu (mg/kg)', 'copper (mg/kg)', 'leaf copper', 'leaf cu', 'leaf_cu'
            ],
            
            'Zn (mg/kg)': [
                'zinc', 'zn', 'zn mg/kg', 'zn_mg_kg', 'zinc mg/kg', 'zinc_mg_kg',
                'zn (mg/kg)', 'zinc (mg/kg)', 'leaf zinc', 'leaf zn', 'leaf_zn'
            ]
        }
        
        # Create reverse mapping for quick lookup
        self.variation_to_standard = {}
        for standard, variations in self.PARAMETER_VARIATIONS.items():
            for variation in variations:
                self.variation_to_standard[variation.lower()] = standard
    
    def standardize_parameter_name(self, param_name: str) -> Optional[str]:
        """
        Convert any parameter name variation to the standard format
        
        Args:
            param_name: The parameter name to standardize
            
        Returns:
            Standard parameter name or None if not found
        """
        if not param_name:
            return None
            
        # Clean the parameter name
        clean_name = param_name.strip().lower()
        clean_name = re.sub(r'\s+', ' ', clean_name)  # Normalize whitespace
        
        # Direct lookup
        if clean_name in self.variation_to_standard:
            return self.variation_to_standard[clean_name]
        
        # Fuzzy matching for partial matches (more precise)
        for variation, standard in self.variation_to_standard.items():
            # Only match if the variation is a significant part of the clean_name
            # Avoid matching single characters or very short variations
            if len(variation) >= 3 and (variation in clean_name or clean_name in variation):
                return standard
        
        return None
    
    def standardize_data_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Standardize all parameter names in a data dictionary
        
        Args:
            data_dict: Dictionary with potentially non-standard parameter names
            
        Returns:
            Dictionary with standardized parameter names
        """
        standardized = {}
        
        for key, value in data_dict.items():
            standard_key = self.standardize_parameter_name(key)
            if standard_key:
                standardized[standard_key] = value
            else:
                # Keep non-parameter keys as-is (like sample_id, lab_no, etc.)
                standardized[key] = value
        
        return standardized
    
    def standardize_samples_list(self, samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Standardize parameter names in a list of sample dictionaries
        
        Args:
            samples: List of sample dictionaries
            
        Returns:
            List of samples with standardized parameter names
        """
        return [self.standardize_data_dict(sample) for sample in samples]
    
    def get_display_name_mapping(self, param_type: str = 'soil') -> Dict[str, str]:
        """
        Get mapping from standard parameter names to display names
        
        Args:
            param_type: 'soil' or 'leaf'
            
        Returns:
            Dictionary mapping standard names to display names
        """
        if param_type.lower() == 'soil':
            return {
                'pH': 'pH',
                'N (%)': 'Nitrogen (%)',
                'Org. C (%)': 'Organic Carbon (%)',
                'Total P (mg/kg)': 'Total P (mg/kg)',
                'Avail P (mg/kg)': 'Available P (mg/kg)',
                'Exch. K (meq/100 g)': 'Exchangeable K (meq/100 g)',
                'Exch. Ca (meq/100 g)': 'Exchangeable Ca (meq/100 g)',
                'Exch. Mg (meq/100 g)': 'Exchangeable Mg (meq/100 g)',
                'CEC (meq/100 g)': 'CEC (meq/100 g)'
            }
        else:  # leaf
            return {
                'N (%)': 'Nitrogen (%)',
                'P (%)': 'Phosphorus (%)',
                'K (%)': 'Potassium (%)',
                'Mg (%)': 'Magnesium (%)',
                'Ca (%)': 'Calcium (%)',
                'B (mg/kg)': 'Boron (mg/kg)',
                'Cu (mg/kg)': 'Copper (mg/kg)',
                'Zn (mg/kg)': 'Zinc (mg/kg)'
            }
    
    def validate_parameter_completeness(self, data_dict: Dict[str, Any], param_type: str = 'soil') -> Dict[str, Any]:
        """
        Validate that all expected parameters are present and add missing ones with default values
        
        Args:
            data_dict: Data dictionary to validate
            param_type: 'soil' or 'leaf'
            
        Returns:
            Dictionary with all expected parameters
        """
        if param_type.lower() == 'soil':
            expected_params = list(self.STANDARD_SOIL_PARAMS.values())
        else:
            expected_params = list(self.STANDARD_LEAF_PARAMS.values())
        
        validated_data = data_dict.copy()
        
        for param in expected_params:
            if param not in validated_data:
                validated_data[param] = 0.0  # Default value for missing parameters
        
        return validated_data
    
    @classmethod
    def get_parameter_variations_mapping(cls) -> Dict[str, List[str]]:
        """Get parameter variations mapping for text analysis"""
        return {
            # Soil Parameters (9)
            'ph': ['ph', 'ph level', 'soil ph', 'acidity', 'alkalinity'],
            'nitrogen': ['nitrogen', 'n', 'n%', 'n_%', 'nitrogen%', 'nitrogen_%', 'n (%)', 'n(%)'],
            'organic_carbon': ['organic carbon', 'organic_carbon', 'carbon', 'c', 'c%', 'c_%', 'organic_carbon_%', 'org. c (%)', 'org c (%)', 'org.c(%)'],
            'total_phosphorus': ['total phosphorus', 'total p', 'total_p', 'total phosphorus mg/kg', 'total_p_mg_kg', 'total p (mg/kg)', 'total p(mg/kg)'],
            'available_phosphorus': ['available phosphorus', 'available p', 'available_p', 'available phosphorus mg/kg', 'available_p_mg_kg', 'avail p (mg/kg)', 'avail p(mg/kg)'],
            'exchangeable_potassium': ['exchangeable potassium', 'exch k', 'exch_k', 'exchangeable k', 'exchangeable_k', 'k meq%', 'k_meq%', 'exchangeable_k_meq%', 'exch. k (meq%)', 'exch k (meq%)', 'exch.k(meq%)'],
            'exchangeable_calcium': ['exchangeable calcium', 'exch ca', 'exch_ca', 'exchangeable ca', 'exchangeable_ca', 'ca meq%', 'ca_meq%', 'exchangeable_ca_meq%', 'exch. ca (meq%)', 'exch ca (meq%)', 'exch.ca(meq%)'],
            'exchangeable_magnesium': ['exchangeable magnesium', 'exch mg', 'exch_mg', 'exchangeable mg', 'exchangeable_mg', 'mg meq%', 'mg_meq%', 'exchangeable_mg_meq%', 'exch. mg (meq%)', 'exch mg (meq%)', 'exch.mg(meq%)'],
            'cec': ['cec', 'cation exchange capacity', 'c.e.c', 'cec meq%', 'cec_meq%', 'cec (meq%)', 'cec(meq%)'],
            
            # Leaf Parameters (8)
            'leaf_nitrogen': ['leaf nitrogen', 'leaf n', 'leaf_n', 'n%', 'n_%', 'nitrogen%', 'nitrogen_%', 'n (%)', 'n(%)'],
            'leaf_phosphorus': ['leaf phosphorus', 'leaf p', 'leaf_p', 'p%', 'p_%', 'phosphorus%', 'phosphorus_%', 'p (%)', 'p(%)'],
            'leaf_potassium': ['leaf potassium', 'leaf k', 'leaf_k', 'k%', 'k_%', 'potassium%', 'potassium_%', 'k (%)', 'k(%)'],
            'leaf_magnesium': ['leaf magnesium', 'leaf mg', 'leaf_mg', 'mg%', 'mg_%', 'magnesium%', 'magnesium_%', 'mg (%)', 'mg(%)'],
            'leaf_calcium': ['leaf calcium', 'leaf ca', 'leaf_ca', 'ca%', 'ca_%', 'calcium%', 'calcium_%', 'ca (%)', 'ca(%)'],
            'leaf_boron': ['leaf boron', 'leaf b', 'leaf_b', 'b mg/kg', 'b_mg_kg', 'boron mg/kg', 'boron_mg_kg', 'b (mg/kg)', 'b(mg/kg)'],
            'leaf_copper': ['leaf copper', 'leaf cu', 'leaf_cu', 'cu mg/kg', 'cu_mg_kg', 'copper mg/kg', 'copper_mg_kg', 'cu (mg/kg)', 'cu(mg/kg)'],
            'leaf_zinc': ['leaf zinc', 'leaf zn', 'leaf_zn', 'zn mg/kg', 'zn_mg_kg', 'zinc mg/kg', 'zinc_mg_kg', 'zn (mg/kg)', 'zn(mg/kg)'],
            
            # Land & Yield Parameters
            'land_size': ['land size', 'land_size', 'farm size', 'farm_size', 'area', 'hectares', 'acres', 'square meters', 'square_meters'],
            'current_yield': ['current yield', 'current_yield', 'yield', 'production', 'tonnes/hectare', 'kg/hectare', 'tonnes/acre', 'kg/acre', 'yield per hectare', 'yield per acre'],
            'yield_forecast': ['yield forecast', 'yield_forecast', 'projected yield', 'projected_yield', 'future yield', 'future_yield', 'yield projection', 'yield_projection'],
            'economic_impact': ['economic impact', 'economic_impact', 'roi', 'return on investment', 'cost benefit', 'cost_benefit', 'profitability', 'revenue', 'income'],
            
            # Legacy mappings for backward compatibility
            'phosphorus': ['phosphorus', 'p', 'p%', 'p_%', 'phosphorus%', 'available p'],
            'potassium': ['potassium', 'k', 'k%', 'k_%', 'potassium%'],
            'calcium': ['calcium', 'ca', 'ca%', 'ca_%', 'calcium%'],
            'magnesium': ['magnesium', 'mg', 'mg%', 'mg_%', 'magnesium%'],
            'copper': ['copper', 'cu', 'cu mg/kg', 'cu_mg/kg', 'copper mg/kg'],
            'zinc': ['zinc', 'zn', 'zn mg/kg', 'zn_mg/kg', 'zinc mg/kg'],
            'boron': ['boron', 'b', 'b mg/kg', 'b_mg/kg', 'boron mg/kg']
        }

# Global instance for easy import
parameter_standardizer = ParameterStandardizer()