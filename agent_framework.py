#!/usr/bin/env python3
"""
UK Personal AI Agent Framework
Autonomous agent with Claude, tool calling, real Miru data
"""

import os
import json
from anthropic import Anthropic
from typing import Optional
import requests
from datetime import datetime
from supabase import create_client, Client
from search import postcode_to_latlon, fetch_all_stations, haversine_km
from sms_service import _get_rtt_token

class UKAgent:
    def __init__(self):
        self.client = Anthropic()
        self.conversation_history = []
        self.user_memory = {}
        self.tools = self._define_tools()

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")
        if supabase_url and supabase_key:
            self.db = create_client(supabase_url, supabase_key)
        else:
            self.db = None

    def _define_tools(self):
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
        if tool_name == "get_trains":
            return self._get_trains(tool_input.get("station"))
        elif tool_name == "get_fuel_prices":
            return self._get_fuel_prices(tool_input.get("postcode"))
        elif tool_name == "get_school_events":
            return self._get_school_events(tool_input.get("school_name"))
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    def _get_trains(self, station: str) -> str:
        """Fetch REAL train departures using Miru's proven RTT code"""
        try:
            station_map = {
                "staines": "STN", "london": "LND", "london waterloo": "WAT",
                "london victoria": "VIC", "chertsey": "CHY", "egham": "EGH",
                "virginia water": "VWW", "longcross": "LCX", "weybridge": "WBR",
                "windsor": "WDS", "reading": "RDG", "kingston": "KNG",
            }
            from_crs = station_map.get(station.lower(), station.upper()[:3])
            self.user_memory["last_station"] = station

            # Use Miru's working RTT integration
            access = _get_rtt_token()
            r = requests.get(
                "https://data.rtt.io/rtt/location",
                headers={"Authorization": f"Bearer {access}"},
                params={"code": f"gb-nr:{from_crs}"},
                timeout=12,
            )
            data = r.json()
            services = data.get("services") or []
            departures = []

            for s in services[:6]:
                loc = s.get("locationDetail", {})
                dep_b = loc.get("gbttBookedDeparture", "")
                dep_r = loc.get("realtimeDeparture", dep_b)
                plat = loc.get("platform", "")
                cancelled = loc.get("cancelledDeparture", False) or loc.get("cancelledCall", False)

                def _fmt(t):
                    t = str(t).strip()
                    if len(t) == 4 and t.isdigit(): return t[:2] + ":" + t[2:]
                    return t[:5] if len(t) >= 5 else t

                departures.append({
                    "time": _fmt(dep_r or dep_b),
                    "destination": s.get("destination", [{}])[-1].get("description", ""),
                    "platform": plat,
                    "cancelled": bool(cancelled),
                    "operator": s.get("atocName", ""),
                })

            return json.dumps({
                "station": station,
                "location": data.get("location", {}).get("name", station),
                "departures": departures[:4],
                "source": "RTT (live real-time)"
            })

        except Exception as e:
            return json.dumps({"station": station, "departures": [], "error": str(e)})

    def _get_fuel_prices(self, postcode: str) -> str:
        """Fetch real fuel prices using Miru's data"""
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
                "source": "Miru fuel station data (live)"
            })

        except Exception as e:
            return json.dumps({"postcode": postcode, "stations": [], "error": str(e)})

    def _get_school_events(self, school_name: str) -> str:
        """Get school events from Miru Supabase"""
        try:
            self.user_memory["last_school"] = school_name

            if self.db:
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
                return json.dumps({"school": school_name, "events": [], "source": "offline"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def chat(self, user_message: str) -> str:
        """Main conversation loop with Claude"""
        self.conversation_history.append({
            "role": "user",
            "content": user_message
        })

        system_prompt = """You are a UK personal AI assistant. You help with trains, fuel prices, schools, and company research.

When user asks about:
- Trains: Use get_trains tool. Show departures, platforms, and status.
- Fuel: Use get_fuel_prices tool. Show cheapest options.
- Schools: Use get_school_events tool. Show upcoming events.
- Company/Brand: Provide analysis based on available data.

Be conversational, remember context, and always provide next steps.
If you've helped before, reference that: "To London Waterloo again?"
"""

        response = self.client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=system_prompt,
            tools=self.tools,
            messages=self.conversation_history
        )

        while response.stop_reason == "tool_use":
            assistant_message = {"role": "assistant", "content": response.content}
            self.conversation_history.append(assistant_message)

            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_name = block.name
                    tool_input = block.input
                    tool_use_id = block.id

                    result = self.call_tool(tool_name, tool_input)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": result
                    })

            self.conversation_history.append({
                "role": "user",
                "content": tool_results
            })

            response = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                system=system_prompt,
                tools=self.tools,
                messages=self.conversation_history
            )

        final_response = ""
        for block in response.content:
            if hasattr(block, "text"):
                final_response = block.text
                break

        self.conversation_history.append({
            "role": "assistant",
            "content": final_response
        })

        return final_response


if __name__ == "__main__":
    agent = UKAgent()
    print("🚂 UK Agent Framework - Test Mode")
    print("Type 'quit' to exit\n")

    while True:
        user_input = input("You: ").strip()
        if user_input.lower() == "quit":
            break

        response = agent.chat(user_input)
        print(f"Agent: {response}\n")
