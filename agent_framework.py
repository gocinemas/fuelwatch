"""
UK Personal AI Agent Framework
Rewrite of Miru using Claude agent architecture.

Handles: trains, fuel, schools, company research, brands
Platform: Web + WhatsApp
"""

import os
import json
from anthropic import Anthropic
from typing import Optional
import requests
from datetime import datetime
from supabase import create_client, Client
from search import postcode_to_latlon, fetch_all_stations, haversine_km

class UKAgent:
    def __init__(self):
        self.client = Anthropic()
        self.conversation_history = []
        self.user_memory = {}  # Persistent user context
        self.tools = self._define_tools()

        # Wire to Miru Supabase
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")
        if supabase_url and supabase_key:
            self.db = create_client(supabase_url, supabase_key)
        else:
            self.db = None

    def _define_tools(self):
        """Define all available tools for Claude"""
        return [
            {
                "name": "get_trains",
                "description": "Get next train departures from a UK station. Returns platform, time, destination, and status.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "station": {
                            "type": "string",
                            "description": "UK station name (e.g., 'Staines', 'London Waterloo')"
                        }
                    },
                    "required": ["station"]
                }
            },
            {
                "name": "get_fuel_prices",
                "description": "Find cheapest fuel prices near a UK postcode. Returns top stations with current prices.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "postcode": {
                            "type": "string",
                            "description": "UK postcode (e.g., 'KT16 0DA')"
                        }
                    },
                    "required": ["postcode"]
                }
            },
            {
                "name": "get_school_events",
                "description": "Get upcoming school events, trips, and reminders for today/this week.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "school_name": {
                            "type": "string",
                            "description": "School name (e.g., 'Stanns Heath')"
                        }
                    },
                    "required": ["school_name"]
                }
            }
        ]

    def call_tool(self, tool_name: str, tool_input: dict) -> str:
        """Execute tool and return results"""
        if tool_name == "get_trains":
            return self._get_trains(tool_input.get("station"))
        elif tool_name == "get_fuel_prices":
            return self._get_fuel_prices(tool_input.get("postcode"))
        elif tool_name == "get_school_events":
            return self._get_school_events(tool_input.get("school_name"))
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    def _get_trains(self, station: str) -> str:
        """Fetch REAL train departures from RTT (same as live Miru)"""
        try:
            self.user_memory["last_station"] = station

            station_map = {
                "staines": "STN", "london": "LND", "london waterloo": "WAT",
                "london victoria": "VIC", "chertsey": "CHY", "egham": "EGH",
                "virginia water": "VWW", "longcross": "LCX", "weybridge": "WBR",
                "windsor": "WDS", "reading": "RDG", "kingston": "KNG",
            }
            from_crs = station_map.get(station.lower(), station.upper()[:3])

            rtt_token = os.getenv("RTT_TOKEN", "")
            if not rtt_token:
                return json.dumps({"station": station, "departures": [], "error": "RTT_TOKEN not set"})

            tr = requests.get("https://data.rtt.io/api/get_access_token",
                headers={"Authorization": f"Bearer {rtt_token}"}, timeout=10)
            access = tr.json().get("token")
            if not access:
                return json.dumps({"station": station, "departures": [], "error": "RTT auth failed"})

            r = requests.get("https://data.rtt.io/rtt/location",
                headers={"Authorization": f"Bearer {access}"},
                params={"code": f"gb-nr:{from_crs}"}, timeout=12)

            services = r.json().get("services") or []
            departures = []

            for s in services[:6]:
                loc = s.get("locationDetail", {})
                dep_b = loc.get("gbttBookedDeparture", "")
                dep_r = loc.get("realtimeDeparture", dep_b)

                if not dep_b and not dep_r:
                    continue

                def _fmt(t):
                    t = str(t).strip()
                    if len(t) == 4 and t.isdigit(): return t[:2] + ":" + t[2:]
                    return t[:5] if len(t) >= 5 else t

                dest = s.get("destination", [{}])
                if dest:
                    dest_name = dest[-1].get("description", dest[-1].get("crs", ""))
                else:
                    dest_name = ""

                departures.append({
                    "time": _fmt(dep_r or dep_b),
                    "destination": dest_name,
                    "platform": loc.get("platform", "TBA"),
                    "operator": s.get("atocName", ""),
                })

            return json.dumps({
                "station": station,
                "crs_code": from_crs,
                "departures": departures,
                "note": "No scheduled departures" if not departures else None,
                "source": "RTT API (live real-time)"
            })

        except Exception as e:
            return json.dumps({"station": station, "departures": [], "error": f"RTT: {str(e)}"})

    def _get_fuel_prices(self, postcode: str) -> str:
        """Fetch real fuel prices (same as live Miru)"""
        try:
            self.user_memory["last_postcode"] = postcode

            coords = postcode_to_latlon(postcode)
            if not coords:
                return json.dumps({"postcode": postcode, "stations": [], "error": "Invalid postcode"})

            lat, lon = coords
            all_stations = fetch_all_stations()

            nearby = []
            for s in all_stations:
                dist = haversine_km(lat, lon, s.get("latitude"), s.get("longitude"))
                if dist <= 10:
                    nearby.append({
                        "name": s.get("name"),
                        "distance_km": round(dist, 1),
                        "petrol_pence": s.get("petrol_price"),
                        "diesel_pence": s.get("diesel_price"),
                        "brand": s.get("brand"),
                    })

            nearby.sort(key=lambda x: x["distance_km"])

            return json.dumps({
                "postcode": postcode,
                "stations": nearby[:5],
                "source": "Miru fuel station data (live - same as miru.humanagency.co)"
            })

        except Exception as e:
            return json.dumps({"postcode": postcode, "stations": [], "error": f"Fuel: {str(e)}"})

    def _get_school_events(self, school_name: str) -> str:
        """Get real school events from Miru Supabase school_events table"""
        try:
            self.user_memory["last_school"] = school_name

            if self.db:
                # Query Miru's school_events table
                response = self.db.table("school_events").select("*").ilike(
                    "school_name", f"%{school_name}%"
                ).order("event_date").limit(10).execute()

                events = response.data if response.data else []
                return json.dumps({
                    "school": school_name,
                    "events": events,
                    "source": "Miru Supabase (school_events)"
                })
            else:
                return json.dumps({
                    "school": school_name,
                    "events": [],
                    "source": "Supabase offline"
                })
        except Exception as e:
            return json.dumps({"error": str(e)})

    def chat(self, user_message: str) -> str:
        """Main conversation loop with Claude"""
        # Add user message to history
        self.conversation_history.append({
            "role": "user",
            "content": user_message
        })

        # Build system prompt
        system_prompt = """You are a UK personal AI assistant. You help with trains, fuel prices, schools, and company research.

When user asks about:
- Trains: Use get_trains tool. Show departures, platforms, and status.
- Fuel: Use get_fuel_prices tool. Show cheapest options.
- Schools: Use get_school_events tool. Show upcoming events.
- Company/Brand: Provide analysis based on available data.

Be conversational, remember context, and always provide next steps.
If you've helped before, reference that: "To London Waterloo again?"
"""

        # Call Claude with tools
        response = self.client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=system_prompt,
            tools=self.tools,
            messages=self.conversation_history
        )

        # Handle tool calls in agentic loop
        while response.stop_reason == "tool_use":
            # Extract tool use from response
            assistant_message = {"role": "assistant", "content": response.content}
            self.conversation_history.append(assistant_message)

            # Find tool calls
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_name = block.name
                    tool_input = block.input
                    tool_use_id = block.id

                    # Execute tool
                    result = self.call_tool(tool_name, tool_input)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": result
                    })

            # Add tool results to history
            self.conversation_history.append({
                "role": "user",
                "content": tool_results
            })

            # Get next response
            response = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                system=system_prompt,
                tools=self.tools,
                messages=self.conversation_history
            )

        # Extract final text response
        final_response = ""
        for block in response.content:
            if hasattr(block, "text"):
                final_response = block.text
                break

        # Add assistant response to history
        self.conversation_history.append({
            "role": "assistant",
            "content": final_response
        })

        return final_response


# Test locally
if __name__ == "__main__":
    agent = UKAgent()

    # Test conversation
    print("🚂 UK Agent Framework - Test Mode")
    print("Type 'quit' to exit\n")

    while True:
        user_input = input("You: ").strip()
        if user_input.lower() == "quit":
            break

        response = agent.chat(user_input)
        print(f"Agent: {response}\n")
