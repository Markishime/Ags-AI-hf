"""
Feedback Learning System for Agricultural Analysis
Allows users to rate and provide feedback on analysis results to improve system performance
"""

import streamlit as st
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from google.cloud.firestore import FieldFilter
# Use our configured Firestore client instead of direct import
import json

# Configure logging
logger = logging.getLogger(__name__)

class FeedbackLearningSystem:
    """Handles user feedback collection and learning system improvements"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.FeedbackLearningSystem")
    
    def collect_feedback(self, analysis_id: str, user_id: str, feedback_data: Dict[str, Any]) -> bool:
        """
        Collect user feedback for an analysis
        
        Args:
            analysis_id: Unique identifier for the analysis
            user_id: User identifier
            feedback_data: Dictionary containing feedback information
            
        Returns:
            bool: True if feedback was saved successfully
        """
        try:
            db = self._get_firestore_client()
            if not db:
                self.logger.error("Failed to get Firestore client")
                return False
            
            # Prepare feedback document
            feedback_doc = {
                'analysis_id': analysis_id,
                'user_id': user_id,
                'timestamp': datetime.now(),
                'overall_rating': feedback_data.get('overall_rating', 0),
                'accuracy_rating': feedback_data.get('accuracy_rating', 0),
                'usefulness_rating': feedback_data.get('usefulness_rating', 0),
                'clarity_rating': feedback_data.get('clarity_rating', 0),
                'step_ratings': feedback_data.get('step_ratings', {}),
                'recommendations_rating': feedback_data.get('recommendations_rating', 0),
                'visualizations_rating': feedback_data.get('visualizations_rating', 0),
                'written_feedback': feedback_data.get('written_feedback', ''),
                'improvement_suggestions': feedback_data.get('improvement_suggestions', ''),
                'would_recommend': feedback_data.get('would_recommend', False),
                'feedback_categories': feedback_data.get('feedback_categories', []),
                'session_data': {
                    'user_agent': feedback_data.get('user_agent', ''),
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            # Save to Firestore
            feedback_ref = db.collection('user_feedback').document()
            feedback_ref.set(feedback_doc)
            
            self.logger.info(f"Feedback saved successfully for analysis {analysis_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving feedback: {str(e)}")
            return False
    
    def get_feedback_analytics(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Get feedback analytics for system improvement
        
        Args:
            days_back: Number of days to look back for analytics
            
        Returns:
            Dict containing analytics data
        """
        try:
            db = self._get_firestore_client()
            if not db:
                return {}
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Query feedback data
            feedback_ref = db.collection('user_feedback')
            feedback_query = feedback_ref.where(filter=FieldFilter('timestamp', '>=', start_date))
            feedback_docs = list(feedback_query.stream())
            
            if not feedback_docs:
                return {
                    'total_feedback': 0,
                    'average_ratings': {},
                    'feedback_trends': {},
                    'improvement_areas': [],
                    'recommendation_rate': 0
                }
            
            # Process feedback data
            total_feedback = len(feedback_docs)
            ratings_sum = {
                'overall': 0,
                'accuracy': 0,
                'usefulness': 0,
                'clarity': 0,
                'recommendations': 0,
                'visualizations': 0
            }
            
            recommendation_count = 0
            feedback_categories = {}
            improvement_suggestions = []
            
            for doc in feedback_docs:
                data = doc.to_dict()
                
                # Sum ratings
                ratings_sum['overall'] += data.get('overall_rating', 0)
                ratings_sum['accuracy'] += data.get('accuracy_rating', 0)
                ratings_sum['usefulness'] += data.get('usefulness_rating', 0)
                ratings_sum['clarity'] += data.get('clarity_rating', 0)
                ratings_sum['recommendations'] += data.get('recommendations_rating', 0)
                ratings_sum['visualizations'] += data.get('visualizations_rating', 0)
                
                # Count recommendations
                if data.get('would_recommend', False):
                    recommendation_count += 1
                
                # Collect feedback categories
                for category in data.get('feedback_categories', []):
                    feedback_categories[category] = feedback_categories.get(category, 0) + 1
                
                # Collect improvement suggestions
                if data.get('improvement_suggestions'):
                    improvement_suggestions.append(data['improvement_suggestions'])
            
            # Calculate averages
            average_ratings = {
                key: round(value / total_feedback, 2) if total_feedback > 0 else 0
                for key, value in ratings_sum.items()
            }
            
            # Calculate recommendation rate
            recommendation_rate = round((recommendation_count / total_feedback) * 100, 2) if total_feedback > 0 else 0
            
            # Identify improvement areas (lowest rated categories)
            improvement_areas = sorted(
                average_ratings.items(),
                key=lambda x: x[1]
            )[:3]
            
            return {
                'total_feedback': total_feedback,
                'average_ratings': average_ratings,
                'feedback_trends': feedback_categories,
                'improvement_areas': improvement_areas,
                'recommendation_rate': recommendation_rate,
                'improvement_suggestions': improvement_suggestions[:10]  # Top 10 suggestions
            }
            
        except Exception as e:
            self.logger.error(f"Error getting feedback analytics: {str(e)}")
            return {}
    
    def get_learning_insights(self) -> Dict[str, Any]:
        """
        Generate learning insights from feedback data for system improvement
        
        Returns:
            Dict containing learning insights and recommendations
        """
        try:
            analytics = self.get_feedback_analytics(days_back=90)  # 3 months of data
            
            if analytics.get('total_feedback', 0) < 5:
                return {
                    'insufficient_data': True,
                    'message': 'Need more feedback data to generate insights'
                }
            
            insights = {
                'system_performance': {
                    'overall_score': analytics['average_ratings'].get('overall', 0),
                    'accuracy_score': analytics['average_ratings'].get('accuracy', 0),
                    'usefulness_score': analytics['average_ratings'].get('usefulness', 0),
                    'clarity_score': analytics['average_ratings'].get('clarity', 0)
                },
                'improvement_priorities': [],
                'strengths': [],
                'recommendations': []
            }
            
            # Identify improvement priorities
            for area, score in analytics['improvement_areas']:
                if score < 3.0:  # Low rating threshold
                    insights['improvement_priorities'].append({
                        'area': area,
                        'current_score': score,
                        'priority': 'high' if score < 2.5 else 'medium'
                    })
            
            # Identify strengths (high ratings)
            for area, score in analytics['average_ratings'].items():
                if score >= 4.0:  # High rating threshold
                    insights['strengths'].append({
                        'area': area,
                        'score': score
                    })
            
            # Generate recommendations based on feedback
            if analytics['average_ratings'].get('accuracy', 0) < 3.5:
                insights['recommendations'].append({
                    'type': 'accuracy',
                    'suggestion': 'Improve data analysis accuracy by enhancing LLM prompts and validation'
                })
            
            if analytics['average_ratings'].get('clarity', 0) < 3.5:
                insights['recommendations'].append({
                    'type': 'clarity',
                    'suggestion': 'Enhance result presentation and explanation clarity'
                })
            
            if analytics['average_ratings'].get('visualizations', 0) < 3.5:
                insights['recommendations'].append({
                    'type': 'visualizations',
                    'suggestion': 'Improve chart and graph quality and relevance'
                })
            
            return insights
            
        except Exception as e:
            self.logger.error(f"Error generating learning insights: {str(e)}")
            return {}
    
    def _get_firestore_client(self):
        """Get Firestore client"""
        try:
            from utils.firebase_config import get_firestore_client
            return get_firestore_client()
        except Exception as e:
            self.logger.error(f"Error getting Firestore client: {str(e)}")
            return None

def display_feedback_section(analysis_id: str, user_id: str):
    """
    Display feedback collection section in the UI
    
    Args:
        analysis_id: Unique identifier for the analysis
        user_id: User identifier
    """
    st.markdown("---")
    
    # Feedback section header
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
        <h3 style="color: white; margin: 0; font-size: 24px; font-weight: 700; text-align: center;">📊 Help Us Improve</h3>
        <p style="color: rgba(255,255,255,0.9); margin: 8px 0 0 0; font-size: 16px; text-align: center;">Your feedback helps us enhance our agricultural analysis system</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize feedback system
    feedback_system = FeedbackLearningSystem()
    
    # Create feedback form
    with st.form("feedback_form", clear_on_submit=True):
        st.markdown("### 🌟 Overall Rating")
        
        # Overall rating
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            overall_rating = st.slider(
                "How would you rate this analysis overall?",
                min_value=1,
                max_value=5,
                value=3,
                help="1 = Poor, 5 = Excellent"
            )
        
        st.markdown("### 📋 Detailed Ratings")
        
        # Detailed ratings in columns
        col1, col2 = st.columns(2)
        
        with col1:
            accuracy_rating = st.slider(
                "Accuracy of Analysis",
                min_value=1,
                max_value=5,
                value=3,
                help="How accurate were the findings and recommendations?"
            )
            
            usefulness_rating = st.slider(
                "Usefulness of Recommendations",
                min_value=1,
                max_value=5,
                value=3,
                help="How useful were the recommendations for your farm?"
            )
            
            clarity_rating = st.slider(
                "Clarity of Presentation",
                min_value=1,
                max_value=5,
                value=3,
                help="How clear and easy to understand were the results?"
            )
        
        with col2:
            recommendations_rating = st.slider(
                "Quality of Recommendations",
                min_value=1,
                max_value=5,
                value=3,
                help="How relevant and actionable were the recommendations?"
            )
            
            visualizations_rating = st.slider(
                "Quality of Charts/Graphs",
                min_value=1,
                max_value=5,
                value=3,
                help="How helpful were the visualizations and charts?"
            )
        
        st.markdown("### 💬 Written Feedback")
        
        # Written feedback
        written_feedback = st.text_area(
            "Please share your thoughts about this analysis:",
            placeholder="What did you find most helpful? What could be improved?",
            height=100
        )
        
        # Improvement suggestions
        improvement_suggestions = st.text_area(
            "Suggestions for improvement:",
            placeholder="Any specific suggestions to make the analysis better?",
            height=80
        )
        
        # Feedback categories
        st.markdown("### 🏷️ Feedback Categories")
        feedback_categories = st.multiselect(
            "What aspects would you like to see improved? (Select all that apply)",
            [
                "Data Accuracy",
                "Recommendation Relevance",
                "Chart Quality",
                "Explanation Clarity",
                "Step-by-Step Analysis",
                "Economic Forecasts",
                "Visualization Design",
                "Technical Details",
                "User Interface",
                "Loading Speed"
            ]
        )
        
        # Recommendation question
        st.markdown("### 👍 Recommendation")
        would_recommend = st.radio(
            "Would you recommend this analysis system to other farmers?",
            ["Yes", "No", "Maybe"],
            horizontal=True
        )
        
        # Submit button
        submitted = st.form_submit_button(
            "Submit Feedback",
            type="primary",
            use_container_width=True
        )
        
        if submitted:
            # Prepare feedback data
            feedback_data = {
                'overall_rating': overall_rating,
                'accuracy_rating': accuracy_rating,
                'usefulness_rating': usefulness_rating,
                'clarity_rating': clarity_rating,
                'recommendations_rating': recommendations_rating,
                'visualizations_rating': visualizations_rating,
                'written_feedback': written_feedback,
                'improvement_suggestions': improvement_suggestions,
                'feedback_categories': feedback_categories,
                'would_recommend': would_recommend == "Yes",
                'user_agent': st.session_state.get('user_agent', 'Unknown')
            }
            
            # Save feedback
            if feedback_system.collect_feedback(analysis_id, user_id, feedback_data):
                st.success("🎉 Thank you for your feedback! Your input helps us improve the system.")
                st.balloons()
            else:
                st.error("❌ There was an error saving your feedback. Please try again.")

def display_feedback_analytics():
    """
    Display feedback analytics for admin users
    """
    st.markdown("### 📊 Feedback Analytics")
    
    feedback_system = FeedbackLearningSystem()
    
    # Get analytics
    analytics = feedback_system.get_feedback_analytics(days_back=30)
    insights = feedback_system.get_learning_insights()
    
    if analytics.get('total_feedback', 0) == 0:
        st.info("No feedback data available yet.")
        return
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Feedback",
            analytics['total_feedback'],
            help="Number of feedback submissions in the last 30 days"
        )
    
    with col2:
        st.metric(
            "Overall Rating",
            f"{analytics['average_ratings'].get('overall', 0):.1f}/5.0",
            help="Average overall rating from users"
        )
    
    with col3:
        st.metric(
            "Recommendation Rate",
            f"{analytics['recommendation_rate']:.1f}%",
            help="Percentage of users who would recommend the system"
        )
    
    with col4:
        st.metric(
            "Accuracy Score",
            f"{analytics['average_ratings'].get('accuracy', 0):.1f}/5.0",
            help="Average accuracy rating from users"
        )
    
    # Display detailed ratings
    st.markdown("#### 📈 Detailed Ratings")
    
    ratings_data = analytics['average_ratings']
    for category, score in ratings_data.items():
        if category != 'overall':
            st.progress(score / 5.0)
            st.write(f"**{category.title()}**: {score:.1f}/5.0")
    
    # Display improvement areas
    if insights.get('improvement_priorities'):
        st.markdown("#### 🎯 Improvement Priorities")
        for priority in insights['improvement_priorities']:
            priority_color = "🔴" if priority['priority'] == 'high' else "🟡"
            st.write(f"{priority_color} **{priority['area'].title()}**: {priority['current_score']:.1f}/5.0")
    
    # Display strengths
    if insights.get('strengths'):
        st.markdown("#### 💪 System Strengths")
        for strength in insights['strengths']:
            st.write(f"✅ **{strength['area'].title()}**: {strength['score']:.1f}/5.0")
    
    # Display recommendations
    if insights.get('recommendations'):
        st.markdown("#### 💡 Improvement Recommendations")
        for rec in insights['recommendations']:
            st.write(f"🔧 **{rec['type'].title()}**: {rec['suggestion']}")
