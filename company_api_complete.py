"""
Complete Company Intelligence API
- Caching
- Real data fetching
- AI Chat
- Intent detection
- Suggested questions
"""

from flask import request, jsonify
from company_intelligence_cache import get_cached_company, cache_company_data, get_top_viewed_companies
from real_company_enricher import get_real_company_data
from company_chat import chat_with_company, detect_user_intent
from company_data_populator import _fetch_wikipedia_summary

def register_company_api_complete(app):
    
    @app.route("/api/company/<company_name>/chat", methods=["POST"])
    def company_chat_endpoint(company_name):
        """AI chat about a company"""
        data = request.json or {}
        question = data.get("question", "").strip()
        
        if not question:
            return jsonify({"error": "Question required"}), 400
        
        try:
            # Get company data (cached or fresh)
            company_data = get_cached_company(company_name)
            if not company_data:
                wiki = _fetch_wikipedia_summary(company_name)
                company_data = get_real_company_data(company_name, wiki.get("extract") if wiki else None)
                cache_company_data(company_name, company_data)
            
            # Detect intent
            intent = detect_user_intent(question)
            
            # Get AI answer
            answer = chat_with_company(company_name, question, company_data)
            
            return jsonify({
                "question": question,
                "answer": answer,
                "intent": intent,
                "company": company_name,
                "cached": get_cached_company(company_name) is not None
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/company/<company_name>/suggested-questions", methods=["GET"])
    def suggested_questions(company_name):
        """Get suggested questions based on company"""
        try:
            company_data = get_cached_company(company_name)
            if not company_data:
                wiki = _fetch_wikipedia_summary(company_name)
                company_data = get_real_company_data(company_name, wiki.get("extract") if wiki else None)
            
            # Generate suggested questions based on available data
            questions = []
            
            if company_data.get("revenue_billions"):
                questions.append(f"How much revenue does {company_name} generate?")
            
            if company_data.get("employees"):
                questions.append(f"How many people work at {company_name}?")
            
            if company_data.get("founded_year"):
                questions.append(f"What's the history of {company_name}?")
            
            questions.extend([
                f"What is {company_name}'s strategy?",
                f"Who are {company_name}'s main competitors?",
                f"What are growth opportunities for {company_name}?",
                f"What sustainability initiatives does {company_name} have?",
            ])
            
            return jsonify({
                "company": company_name,
                "suggested_questions": questions[:5]
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/companies/trending", methods=["GET"])
    def trending_companies():
        """Get most viewed/trending companies"""
        try:
            companies = get_top_viewed_companies(limit=10)
            return jsonify({"trending": companies})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("Company API complete module loaded")
