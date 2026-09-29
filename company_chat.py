"""
Company Intelligence Chat - Ask questions about any company.
AI assistant provides targeted answers based on company data.
"""

import os
from groq import Groq

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

def chat_with_company(company_name: str, question: str, company_context: dict) -> str:
    """
    Chat with AI about a company.
    Uses company data as context for accurate answers.
    """
    if not GROQ_API_KEY:
        return "Chat not available - AI service not configured"
    
    try:
        client = Groq(api_key=GROQ_API_KEY)
        
        # Build context from company data
        context = f"""
You are an expert company analyst. Answer questions about {company_name} based on this data:

Name: {company_context.get('name', 'N/A')}
Founded: {company_context.get('founded_year', 'N/A')}
Headquarters: {company_context.get('headquarters', 'N/A')}
Revenue: ${company_context.get('revenue_billions', 'N/A')}B
Employees: {company_context.get('employees', 'N/A'):,}
Description: {company_context.get('description', 'N/A')}

Be specific and factual. If you don't know, say so."""
        
        response = client.messages.create(
            model="mixtral-8x7b-32768",
            messages=[
                {"role": "user", "content": context},
                {"role": "user", "content": f"Question: {question}"}
            ],
            temperature=0.7,
            max_tokens=500
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"[chat] Failed: {e}")
        return f"Error: {str(e)}"

def detect_user_intent(question: str) -> dict:
    """Detect what user wants to understand about company"""
    intents = {
        "financials": ["revenue", "profit", "earnings", "income", "assets", "cash"],
        "people": ["employees", "team", "ceo", "founder", "leadership", "workforce"],
        "strategy": ["strategy", "focus", "roadmap", "plans", "goals", "vision"],
        "products": ["products", "brands", "services", "offerings", "portfolio"],
        "market": ["competitors", "market share", "competition", "industry", "position"],
        "growth": ["grow", "expansion", "growth", "scale", "acquire"],
        "sustainability": ["environmental", "esg", "green", "sustainability", "carbon"]
    }
    
    question_lower = question.lower()
    detected = []
    
    for intent, keywords in intents.items():
        if any(kw in question_lower for kw in keywords):
            detected.append(intent)
    
    return {"intents": detected, "primary": detected[0] if detected else "general"}

if __name__ == "__main__":
    context = {
        "name": "Apple Inc.",
        "founded_year": 1976,
        "employees": 164000,
        "revenue_billions": 383
    }
    
    answer = chat_with_company("Apple", "What is Apple's strategy?", context)
    print(answer)
    
    intent = detect_user_intent("How many employees does Apple have?")
    print(f"Intent: {intent}")
