import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
from firebase_config import get_firestore_client, COLLECTIONS
import streamlit as st

# Default AI Configuration Schema
DEFAULT_AI_CONFIG = {
    'prompt_templates': {
        'analysis_template': {
            'name': 'Standard Analysis Template',
            'description': 'Main template for analyzing lab data',
            'template': """
This is an expert agricultural analysis system for oil palm cultivation and nutrition analysis using neutral, third-person language only.

Based on the following lab analysis data and MPOB standards, provide a comprehensive analysis:

Lab Data:
{lab_data}

Report Type: {report_type}

MPOB Standards:
{mpob_standards}

Reference Materials:
{reference_materials}

Additional Context:
{rag_context}

Please provide:
1. Parameter Analysis: Compare each parameter against MPOB standards
2. Issues Identified: List any deficiencies or excesses
3. Recommendations: Specific fertilizer and management recommendations
4. Economic Impact: Estimated costs and potential yield impact
5. Priority Actions: Most critical actions to take first

Format your response according to the specified output format: {output_format}
Use the following tags for categorization: {tags}
""",
            'placeholders': [
                {'name': 'lab_data', 'description': 'Laboratory analysis data', 'required': True},
                {'name': 'report_type', 'description': 'Type of report (soil/leaf)', 'required': True},
                {'name': 'mpob_standards', 'description': 'MPOB standards for comparison', 'required': True},
                {'name': 'reference_materials', 'description': 'Additional reference materials', 'required': False},
                {'name': 'rag_context', 'description': 'RAG context from knowledge base', 'required': False},
                {'name': 'output_format', 'description': 'Desired output format', 'required': False},
                {'name': 'tags', 'description': 'Tags for categorization', 'required': False}
            ],
            'active': True,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        },
        'recommendation_template': {
            'name': 'Recommendation Template',
            'description': 'Template for generating specific recommendations',
            'template': """
Based on the analysis of {report_type} data, provide specific recommendations:

Issues Found:
{issues}

Reference Materials Available:
{reference_materials}

For each issue, provide:
1. Specific fertilizer type and application rate
2. Application timing and method
3. Expected cost per palm
4. Expected improvement timeline
5. Monitoring recommendations

Consider MPOB best practices and current market conditions.
Format according to: {output_format}
Apply tags: {tags}
""",
            'placeholders': [
                {'name': 'report_type', 'description': 'Type of report', 'required': True},
                {'name': 'issues', 'description': 'Identified issues', 'required': True},
                {'name': 'reference_materials', 'description': 'Reference materials', 'required': False},
                {'name': 'output_format', 'description': 'Output format', 'required': False},
                {'name': 'tags', 'description': 'Tags', 'required': False}
            ],
            'active': True,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
    },
    'reference_materials': {
        'mpob_guidelines': {
            'name': 'MPOB Official Guidelines',
            'type': 'document',
            'description': 'Official MPOB guidelines for oil palm cultivation',
            'content': 'MPOB standards and best practices for oil palm nutrition management.',
            'url': '',
            'tags': ['mpob', 'standards', 'guidelines'],
            'active': True,
            'priority': 'high',
            'created_at': datetime.now()
        },
        'fertilizer_recommendations': {
            'name': 'Fertilizer Application Guide',
            'type': 'knowledge_base',
            'description': 'Comprehensive guide for fertilizer applications',
            'content': 'Detailed fertilizer application rates, timing, and methods for oil palm.',
            'url': '',
            'tags': ['fertilizer', 'application', 'guide'],
            'active': True,
            'priority': 'high',
            'created_at': datetime.now()
        }
    },
    'output_formats': {
        'standard_report': {
            'name': 'Standard Analysis Report',
            'description': 'Standard format for analysis reports',
            'format': {
                'sections': [
                    {'name': 'Executive Summary', 'required': True, 'order': 1},
                    {'name': 'Parameter Analysis', 'required': True, 'order': 2},
                    {'name': 'Issues Identified', 'required': True, 'order': 3},
                    {'name': 'Recommendations', 'required': True, 'order': 4},
                    {'name': 'Economic Impact', 'required': False, 'order': 5},
                    {'name': 'Implementation Timeline', 'required': False, 'order': 6}
                ],
                'styling': {
                    'use_emojis': True,
                    'use_tables': True,
                    'use_bullet_points': True,
                    'include_charts': False
                },
                'export_options': ['pdf', 'html', 'markdown']
            },
            'active': True,
            'created_at': datetime.now()
        },
        'detailed_report': {
            'name': 'Detailed Technical Report',
            'description': 'Comprehensive technical analysis report',
            'format': {
                'sections': [
                    {'name': 'Executive Summary', 'required': True, 'order': 1},
                    {'name': 'Methodology', 'required': True, 'order': 2},
                    {'name': 'Parameter Analysis', 'required': True, 'order': 3},
                    {'name': 'Statistical Analysis', 'required': True, 'order': 4},
                    {'name': 'Issues Identified', 'required': True, 'order': 5},
                    {'name': 'Recommendations', 'required': True, 'order': 6},
                    {'name': 'Economic Impact', 'required': True, 'order': 7},
                    {'name': 'Risk Assessment', 'required': True, 'order': 8},
                    {'name': 'Implementation Timeline', 'required': True, 'order': 9},
                    {'name': 'References', 'required': True, 'order': 10}
                ],
                'styling': {
                    'use_emojis': False,
                    'use_tables': True,
                    'use_bullet_points': True,
                    'include_charts': True
                },
                'export_options': ['pdf', 'html', 'docx']
            },
            'active': True,
            'created_at': datetime.now()
        }
    },
    'tagging_config': {
        'categories': {
            'severity': {
                'name': 'Issue Severity',
                'description': 'Categorize issues by severity level',
                'tags': [
                    {'name': 'critical', 'color': '#FF0000', 'description': 'Critical issues requiring immediate attention'},
                    {'name': 'high', 'color': '#FF6600', 'description': 'High priority issues'},
                    {'name': 'medium', 'color': '#FFCC00', 'description': 'Medium priority issues'},
                    {'name': 'low', 'color': '#00FF00', 'description': 'Low priority issues'}
                ],
                'active': True
            },
            'nutrient_type': {
                'name': 'Nutrient Type',
                'description': 'Categorize by nutrient type',
                'tags': [
                    {'name': 'macronutrient', 'color': '#0066CC', 'description': 'Primary macronutrients (N, P, K)'},
                    {'name': 'secondary', 'color': '#6600CC', 'description': 'Secondary nutrients (Ca, Mg, S)'},
                    {'name': 'micronutrient', 'color': '#CC6600', 'description': 'Micronutrients (B, Cu, Zn, etc.)'},
                    {'name': 'soil_property', 'color': '#666666', 'description': 'Soil properties (pH, CEC, etc.)'}
                ],
                'active': True
            },
            'action_type': {
                'name': 'Action Type',
                'description': 'Categorize recommendations by action type',
                'tags': [
                    {'name': 'fertilization', 'color': '#009900', 'description': 'Fertilizer applications'},
                    {'name': 'soil_management', 'color': '#996633', 'description': 'Soil management practices'},
                    {'name': 'monitoring', 'color': '#0099CC', 'description': 'Monitoring and testing'},
                    {'name': 'maintenance', 'color': '#CC9900', 'description': 'Maintenance activities'}
                ],
                'active': True
            }
        },
        'auto_tagging': {
            'enabled': True,
            'rules': [
                {'condition': 'pH < 4.5', 'tags': ['critical', 'soil_property', 'soil_management']},
                {'condition': 'N < 2.4', 'tags': ['high', 'macronutrient', 'fertilization']},
                {'condition': 'P < 0.15', 'tags': ['high', 'macronutrient', 'fertilization']},
                {'condition': 'K < 1.0', 'tags': ['high', 'macronutrient', 'fertilization']}
            ]
        }
    },
    'general_settings': {
        'ai_model': 'gemini-2.5-pro',
        'temperature': 0.7,
        'max_tokens': 65536,
        'include_confidence_scores': True,
        'enable_rag': True,
        'auto_save_results': True,
        'notification_settings': {
            'email_reports': False,
            'critical_alerts': True
        }
    },
    'created_at': datetime.now(),
    'updated_at': datetime.now(),
    'version': '1.0'
}

def load_ai_configuration() -> Dict[str, Any]:
    """Load AI configuration from Firestore"""
    try:
        db = get_firestore_client()
        if not db:
            return DEFAULT_AI_CONFIG
        
        config_ref = db.collection(COLLECTIONS['ai_configuration']).document('default')
        config_doc = config_ref.get()
        
        if config_doc.exists:
            return config_doc.to_dict()
        else:
            # Initialize with default configuration
            save_ai_configuration(DEFAULT_AI_CONFIG)
            return DEFAULT_AI_CONFIG
            
    except Exception as e:
        st.error(f"Error loading AI configuration: {str(e)}")
        return DEFAULT_AI_CONFIG

def save_ai_configuration(config_data: Dict[str, Any]) -> bool:
    """Save AI configuration to Firestore"""
    try:
        db = get_firestore_client()
        if not db:
            return False
        
        config_data['updated_at'] = datetime.now()
        config_ref = db.collection(COLLECTIONS['ai_configuration']).document('default')
        config_ref.set(config_data)
        
        return True
        
    except Exception as e:
        st.error(f"Error saving AI configuration: {str(e)}")
        return False

def get_prompt_template(template_name: str) -> Dict[str, Any]:
    """Get specific prompt template"""
    config = load_ai_configuration()
    return config.get('prompt_templates', {}).get(template_name, {})

def get_reference_materials(active_only: bool = True) -> Dict[str, Any]:
    """Get reference materials"""
    config = load_ai_configuration()
    materials = config.get('reference_materials', {})
    
    if active_only:
        return {k: v for k, v in materials.items() if v.get('active', False)}
    
    return materials

def get_output_format(format_name: str) -> Dict[str, Any]:
    """Get specific output format"""
    config = load_ai_configuration()
    return config.get('output_formats', {}).get(format_name, {})

def get_tagging_config() -> Dict[str, Any]:
    """Get tagging configuration"""
    config = load_ai_configuration()
    return config.get('tagging_config', {})

def validate_prompt_template(template_content: str, placeholders: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate a prompt template for correctness and completeness"""
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not template_content.strip():
        validation_result['valid'] = False
        validation_result['errors'].append("Template content cannot be empty")
        return validation_result
    
    # Find all placeholders in template
    template_placeholders = re.findall(r'\{([^}]+)\}', template_content)
    
    # Check for required placeholders
    required_placeholders = [ph['name'] for ph in placeholders if ph.get('required', False)]
    
    for required_ph in required_placeholders:
        if required_ph not in template_placeholders:
            validation_result['valid'] = False
            validation_result['errors'].append(f"Required placeholder '{required_ph}' is missing from template")
    
    # Check for undefined placeholders
    defined_placeholders = [ph['name'] for ph in placeholders]
    
    for template_ph in template_placeholders:
        if template_ph not in defined_placeholders:
            validation_result['warnings'].append(f"Placeholder '{template_ph}' is used in template but not defined in configuration")
    
    # Check for unused placeholders
    for defined_ph in defined_placeholders:
        if defined_ph not in template_placeholders:
            validation_result['warnings'].append(f"Placeholder '{defined_ph}' is defined but not used in template")
    
    # Check template structure
    if len(template_content) < 50:
        validation_result['warnings'].append("Template seems very short. Consider adding more detailed instructions.")
    
    # Check for common issues
    if 'analyze' not in template_content.lower() and 'analysis' not in template_content.lower():
        validation_result['warnings'].append("Template doesn't seem to contain analysis instructions")
    
    return validation_result

def reset_ai_configuration() -> bool:
    """Reset AI configuration to defaults"""
    return save_ai_configuration(DEFAULT_AI_CONFIG)