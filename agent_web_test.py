from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import json

app = FastAPI()

@app.post("/api/chat")
async def chat(request: Request):
    data = await request.json()
    message = data.get("message", "").lower()
    
    # Mock responses
    if "train" in message:
        response = "Next trains from Staines:\n20:29 - London Waterloo (Platform 1)\n20:33 - London Waterloo (Platform 1)\n20:38 - London Waterloo (Platform 1)"
    elif "fuel" in message:
        response = "Cheapest fuel near you:\n1. Shell Chertsey - £1.28/L (2.1km away)\n2. BP Ottershaw - £1.26/L (3.4km away)\n3. Tesco Chobham - £1.25/L (4.2km away)"
    else:
        response = "I can help with trains, fuel, schools, and company research. What would you like to know?"
    
    return JSONResponse({"response": response})

@app.get("/", response_class=HTMLResponse)
async def home():
    return open("/Users/srevi/fuelwatch/agent_web.py").read().split('"""')[2].split('return """')[1].split('"""')[0]

if __name__ == "__main__":
    import uvicorn
    print("🚀 Mock server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
