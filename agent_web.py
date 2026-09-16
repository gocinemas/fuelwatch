"""
Web API for UK Agent Framework
FastAPI server with web chat interface
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from agent_framework import UKAgent
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

app = FastAPI()
agent = UKAgent()

# Store session agents per user (simple implementation)
sessions = {}

@app.post("/api/chat")
async def chat(request: Request):
    """Handle chat messages"""
    try:
        data = await request.json()
        message = data.get("message", "")

        if not message:
            return JSONResponse({"error": "Empty message"}, status_code=400)

        # Get or create user session
        session_id = data.get("session_id", "default")
        if session_id not in sessions:
            sessions[session_id] = UKAgent()

        # Get response from agent
        response = sessions[session_id].chat(message)

        return JSONResponse({
            "response": response,
            "session_id": session_id
        })

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/", response_class=HTMLResponse)
async def home():
    """Serve web chat interface"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>UK Agent - Web Chat</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; }
            .container { max-width: 800px; margin: 0 auto; height: 100vh; display: flex; flex-direction: column; }
            .header { padding: 20px; background: #007AFF; color: white; text-align: center; }
            .header h1 { font-size: 24px; }
            .header p { font-size: 12px; opacity: 0.8; margin-top: 5px; }

            .chat-box { flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }
            .message { padding: 12px 16px; border-radius: 12px; max-width: 80%; word-wrap: break-word; }
            .user-msg { align-self: flex-end; background: #007AFF; color: white; }
            .agent-msg { align-self: flex-start; background: #e5e5ea; color: black; }

            .input-area { padding: 20px; background: white; border-top: 1px solid #ddd; display: flex; gap: 10px; }
            .input-area input { flex: 1; padding: 12px; border: 1px solid #ddd; border-radius: 24px; font-size: 16px; }
            .input-area button { padding: 12px 24px; background: #007AFF; color: white; border: none; border-radius: 24px; cursor: pointer; font-weight: 600; }
            .input-area button:hover { background: #0051D5; }

            .loading { display: none; }
            .loading.active { display: inline; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🇬🇧 UK Personal Agent</h1>
                <p>Ask about trains, fuel, schools, companies, brands...</p>
            </div>

            <div class="chat-box" id="chatBox"></div>

            <div class="input-area">
                <input type="text" id="userInput" placeholder="Type your question..." />
                <button onclick="sendMessage()">Send</button>
            </div>
        </div>

        <script>
            const sessionId = "user-" + Date.now();

            function addMessage(text, isUser = false) {
                const chatBox = document.getElementById("chatBox");
                const msg = document.createElement("div");
                msg.className = isUser ? "message user-msg" : "message agent-msg";
                msg.textContent = text;
                chatBox.appendChild(msg);
                chatBox.scrollTop = chatBox.scrollHeight;
            }

            async function sendMessage() {
                const input = document.getElementById("userInput");
                const message = input.value.trim();

                if (!message) return;

                // Add user message
                addMessage(message, true);
                input.value = "";

                // Show loading
                addMessage("Thinking...", false);

                try {
                    const response = await fetch("/api/chat", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ message, session_id: sessionId })
                    });

                    const data = await response.json();

                    // Remove loading, add response
                    const chatBox = document.getElementById("chatBox");
                    chatBox.removeChild(chatBox.lastChild);
                    addMessage(data.response, false);

                } catch (error) {
                    addMessage("Error: " + error.message, false);
                }
            }

            // Allow Enter to send
            document.getElementById("userInput").addEventListener("keypress", (e) => {
                if (e.key === "Enter") sendMessage();
            });

            // Welcome message
            window.addEventListener("load", () => {
                addMessage("Hi! I'm your UK personal AI. Try asking me about trains from Staines, fuel prices, school events, or company research. What can I help with?", false);
            });
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting UK Agent Web Server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
