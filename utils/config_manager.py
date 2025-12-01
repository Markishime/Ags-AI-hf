# Configuration manager
import json
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class AIConfig:
    """AI configuration settings"""
    model: str = "gemini-2.5-pro"
    embedding_model: str = "text-embedding-004"
    temperature: float = 0.0
    max_tokens: int = 1000
    top_p: float = 0.9
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    enable_rag: bool = True
    enable_caching: bool = True
    retry_attempts: int = 3
    timeout_seconds: int = 30
    confidence_threshold: float = 0.8

@dataclass
class MPOBStandard:
    """MPOB standard definition"""
    parameter: str
    min_value: float
    max_value: float
    unit: str
    optimal_value: float = None
    description: str = ""
    critical: bool = False

@dataclass
class MPOBStandards:
    """MPOB standards collection"""
    standards: Dict[str, MPOBStandard] = None
    soil_standards: Dict[str, MPOBStandard] = None
    leaf_standards: Dict[str, MPOBStandard] = None
    
    def __post_init__(self):
        if self.standards is None:
            self.standards = {}
        if self.soil_standards is None:
            self.soil_standards = {}
        if self.leaf_standards is None:
            self.leaf_standards = {}

@dataclass
class EconomicConfig:
    """Economic configuration"""
    currency: str = "MYR"
    yield_price_per_ton: float = 2500.0
    inflation_rate: float = 0.03
    region: str = "Malaysia"
    discount_rate: float = 0.08
    fertilizer_costs: Dict[str, float] = None
    application_costs: Dict[str, float] = None
    labor_costs: Dict[str, float] = None
    equipment_costs: Dict[str, float] = None
    
    def __post_init__(self):
        if self.fertilizer_costs is None:
            self.fertilizer_costs = {
                "NPK": 1200.0,
                "Urea": 800.0,
                "Rock Phosphate": 600.0,
                "Potash": 1000.0
            }
        if self.application_costs is None:
            self.application_costs = {
                "Manual": 50.0,
                "Mechanical": 100.0,
                "Aerial": 200.0
            }
        if self.labor_costs is None:
            self.labor_costs = {
                "Skilled": 80.0,
                "Unskilled": 40.0,
                "Supervisor": 120.0
            }
        if self.equipment_costs is None:
            self.equipment_costs = {
                "Tractor": 200.0,
                "Sprayer": 80.0,
                "Fertilizer Spreader": 150.0
            }

@dataclass
class OCRConfig:
    """OCR configuration"""
    confidence_threshold: float = 0.8
    language: str = "en"
    preprocessing: bool = True
    psm_modes: list = None
    character_whitelist: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.,-:()%"
    scale_factor_min: float = 1.0
    scale_factor_max: float = 2.0
    contrast_enhancement: float = 1.2
    sharpness_enhancement: float = 1.1
    bilateral_filter_d: int = 9
    bilateral_filter_sigma_color: float = 75.0
    bilateral_filter_sigma_space: float = 75.0
    adaptive_threshold_block_size: int = 11
    adaptive_threshold_c: float = 2.0
    
    def __post_init__(self):
        if self.psm_modes is None:
            self.psm_modes = [6, 7, 8, 13]

@dataclass
class UIConfig:
    """UI configuration"""
    theme: str = "light"
    sidebar_expanded: bool = True
    show_advanced: bool = False
    primary_color: str = "#4CAF50"
    secondary_color: str = "#2196F3"
    accent_color: str = "#FF9800"
    language: str = "en"
    date_format: str = "%Y-%m-%d"
    number_format: str = "%.2f"
    units: Dict[str, list] = None
    display_preferences: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.units is None:
            self.units = {
                "land_size": ["hectares", "acres"],
                "yield": ["tonnes/hectare", "kg/hectare"]
            }
        if self.display_preferences is None:
            self.display_preferences = {
                "show_icons": True,
                "show_colors": True,
                "compact_mode": False,
                "auto_refresh": True,
                "default_chart_type": "line"
            }

class ConfigManager:
    """Configuration manager for the application"""
    
    def __init__(self):
        self.config_dir = "config"
        self._cache = {}  # Initialize cache
        self.ensure_config_dir()
    
    def ensure_config_dir(self):
        """Ensure config directory exists"""
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)
    
    def get_ai_config(self) -> AIConfig:
        """Get AI configuration"""
        return AIConfig()
    
    def get_mpob_standards(self) -> MPOBStandards:
        """Get MPOB standards"""
        # Create default MPOB standards based on official MPOB oil palm optimal values
        # 9 Soil parameters - matching analysis file parameter names
        default_soil_standards = {
            "pH": MPOBStandard("pH", 4.5, 5.5, "pH units", 5.0, "Soil pH level for optimal oil palm growth", True),
            "N (%)": MPOBStandard("N (%)", 0.15, 0.25, "%", 0.20, "Total nitrogen content in soil", True),
            "Org. C (%)": MPOBStandard("Org. C (%)", 2.0, 4.0, "%", 3.0, "Organic carbon content in soil", True),
            "Total P (mg/kg)": MPOBStandard("Total P (mg/kg)", 15.0, 30.0, "mg/kg", 22.5, "Total phosphorus content", True),
            "Avail P (mg/kg)": MPOBStandard("Avail P (mg/kg)", 15.0, 30.0, "mg/kg", 22.5, "Available phosphorus (Bray-1)", True),
            "Exch. K (meq%)": MPOBStandard("Exch. K (meq%)", 0.15, 0.30, "meq%", 0.225, "Exchangeable potassium", True),
            "Exch. Ca (meq%)": MPOBStandard("Exch. Ca (meq%)", 2.0, 8.0, "meq%", 5.0, "Exchangeable calcium", False),
            "Exch. Mg (meq%)": MPOBStandard("Exch. Mg (meq%)", 0.5, 2.0, "meq%", 1.25, "Exchangeable magnesium", False),
            "CEC (meq%)": MPOBStandard("CEC (meq%)", 8.0, 20.0, "meq%", 14.0, "Soil cation exchange capacity", False)
        }
        
        # 8 Leaf parameters - matching analysis file parameter names
        default_leaf_standards = {
            "N (%)": MPOBStandard("N (%)", 2.4, 2.8, "%", 2.6, "Leaf nitrogen content (frond 17)", True),
            "P (%)": MPOBStandard("P (%)", 0.15, 0.25, "%", 0.20, "Leaf phosphorus content (frond 17)", True),
            "K (%)": MPOBStandard("K (%)", 1.0, 1.3, "%", 1.15, "Leaf potassium content (frond 17)", True),
            "Ca (%)": MPOBStandard("Ca (%)", 0.5, 1.0, "%", 0.75, "Leaf calcium content (frond 17)", False),
            "Mg (%)": MPOBStandard("Mg (%)", 0.25, 0.50, "%", 0.375, "Leaf magnesium content (frond 17)", False),
            "B (mg/kg)": MPOBStandard("B (mg/kg)", 10.0, 25.0, "mg/kg", 17.5, "Leaf boron content (frond 17)", False),
            "Cu (mg/kg)": MPOBStandard("Cu (mg/kg)", 5.0, 15.0, "mg/kg", 10.0, "Leaf copper content (frond 17)", False),
            "Zn (mg/kg)": MPOBStandard("Zn (mg/kg)", 15.0, 30.0, "mg/kg", 22.5, "Leaf zinc content (frond 17)", False)
        }
        
        return MPOBStandards(
            standards={},
            soil_standards=default_soil_standards,
            leaf_standards=default_leaf_standards
        )
    
    def get_economic_config(self) -> EconomicConfig:
        """Get economic configuration"""
        return EconomicConfig()
    
    def get_ocr_config(self) -> OCRConfig:
        """Get OCR configuration"""
        return OCRConfig()
    
    def get_ui_config(self) -> UIConfig:
        """Get UI configuration"""
        return UIConfig()
    
    def save_config(self, config_type: str, config_data: Dict[str, Any]) -> bool:
        """Save configuration"""
        try:
            config_path = os.path.join(self.config_dir, f"{config_type}.json")
            with open(config_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            return True
        except Exception:
            return False
    
    def load_config(self, config_type: str) -> Optional[Dict[str, Any]]:
        """Load configuration"""
        try:
            config_path = os.path.join(self.config_dir, f"{config_type}.json")
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return json.load(f)
            return None
        except Exception:
            return None
    
    def reset_to_defaults(self, config_type: str) -> bool:
        """Reset configuration to defaults"""
        try:
            config_path = os.path.join(self.config_dir, f"{config_type}.json")
            if os.path.exists(config_path):
                os.remove(config_path)
            return True
        except Exception:
            return False
    
    def get_all_configs(self) -> Dict[str, Any]:
        """Get all configuration objects"""
        return {
            'ai_config': self.get_ai_config(),
            'mpob_standards': self.get_mpob_standards(),
            'economic_config': self.get_economic_config(),
            'ocr_config': self.get_ocr_config(),
            'ui_config': self.get_ui_config()
        }
    
    def clear_cache(self) -> bool:
        """Clear the configuration cache"""
        try:
            self._cache.clear()
            return True
        except Exception:
            return False

# Global config manager instance
config_manager = ConfigManager()

def get_ui_config() -> UIConfig:
    """Get UI configuration"""
    return config_manager.get_ui_config()

def get_ai_config() -> AIConfig:
    """Get AI configuration"""
    return config_manager.get_ai_config()

def get_mpob_standards() -> MPOBStandards:
    """Get MPOB standards"""
    return config_manager.get_mpob_standards()

def get_economic_config() -> EconomicConfig:
    """Get economic configuration"""
    return config_manager.get_economic_config()