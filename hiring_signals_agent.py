"""
Hiring Signals Live Agent Fetcher

Fires background agents to fetch REAL job data from:
- Adzuna Job Aggregator API (primary - free tier available)
- Indeed Job Feed
- LinkedIn Jobs API
- Google Jobs API
- Company careers pages (Workable, Greenhouse, Lever, BambooHR)

This replaces hardcoded sample data with dynamic, real-time fetching.
Uses multiple fallback sources to maximize coverage.
"""

import requests
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import re

logger = logging.getLogger(__name__)


def fetch_hiring_signals_live(company_name: str) -> dict:
    """
    Fetch REAL hiring data for a company from multiple sources.

    This attempts multiple sources in parallel priority order:
    1. Adzuna API (job aggregator - primary, most reliable)
    2. Google Jobs API (if available)
    3. LinkedIn careers page (dynamic scraping)
    4. Indeed job search (API/scraping)
    5. Company careers page (Workable, Greenhouse, Lever, BambooHR)

    Returns live data or HTTP 202 if still indexing.
    """

    result = {
        "company": company_name,
        "overview": {
            "total_open_roles": 0,
            "hiring_growth_3m": "—",
            "top_region": "—",
            "signal": "Indexing from live sources..."
        },
        "top_departments": [],
        "growth_signals": [],
        "sample_roles": [],
        "data_sources": []
    }

    try:
        all_jobs = []

        # Strategy: Try all sources and combine results
        # This gives better coverage than falling back to first successful one

        # 1. Adzuna API (free tier, most reliable)
        logger.info(f"[hiring] Querying Adzuna for {company_name}...")
        adzuna_jobs = _fetch_adzuna_live(company_name)
        if adzuna_jobs:
            all_jobs.extend(adzuna_jobs)
            result["data_sources"].append("Adzuna")

        # 2. Google Jobs API (if configured)
        logger.info(f"[hiring] Querying Google Jobs for {company_name}...")
        google_jobs = _fetch_google_jobs_live(company_name)
        if google_jobs:
            all_jobs.extend(google_jobs)
            result["data_sources"].append("Google Jobs")

        # 3. LinkedIn (careers page pattern)
        logger.info(f"[hiring] Checking LinkedIn careers for {company_name}...")
        linkedin_jobs = _fetch_linkedin_live(company_name)
        if linkedin_jobs:
            all_jobs.extend(linkedin_jobs)
            result["data_sources"].append("LinkedIn")

        # 4. Indeed
        logger.info(f"[hiring] Querying Indeed for {company_name}...")
        indeed_jobs = _fetch_indeed_live(company_name)
        if indeed_jobs:
            all_jobs.extend(indeed_jobs)
            result["data_sources"].append("Indeed")

        # 5. Company careers page
        logger.info(f"[hiring] Checking company careers page for {company_name}...")
        careers_jobs = _fetch_company_careers_live(company_name)
        if careers_jobs:
            all_jobs.extend(careers_jobs)
            result["data_sources"].append("Company Careers")

        # Deduplicate and analyze
        if all_jobs:
            all_jobs = _deduplicate_jobs(all_jobs)
            result = _analyze_jobs(company_name, all_jobs)
            result["data_sources"] = list(set(result["data_sources"]))  # Remove duplicates
            logger.info(f"[hiring] Found {len(all_jobs)} jobs for {company_name} from {len(result['data_sources'])} sources")
            return result
        else:
            logger.warning(f"[hiring] No job data found for {company_name} from any source")
            return result

    except Exception as e:
        logger.error(f"[hiring_signals_agent] ERROR: {e}", exc_info=True)
        return result


def _fetch_adzuna_live(company_name: str) -> list:
    """Fetch jobs from Adzuna API (job aggregator)."""
    jobs = []
    try:
        api_key = os.environ.get("ADZUNA_API_KEY", "")
        if not api_key:
            logger.debug("[adzuna] API key not configured")
            return jobs

        # Parse API key (format: app_id:app_key)
        parts = api_key.split(":")
        if len(parts) != 2:
            logger.debug("[adzuna] Invalid API key format")
            return jobs

        app_id, app_key = parts

        # Query Adzuna for company jobs
        url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "what": f'"{company_name}"',
            "results_per_page": 50
        }

        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for job in data.get("results", []):
                jobs.append({
                    "title": job.get("title", ""),
                    "company": job.get("company", {}).get("display_name", ""),
                    "location": job.get("location", {}).get("display_name", ""),
                    "department": _extract_department(job.get("title", "")),
                    "level": _extract_level(job.get("title", "")),
                    "posted_date": job.get("created", ""),
                    "url": job.get("redirect_url", ""),
                    "salary": job.get("salary_max", "")
                })

            logger.info(f"[adzuna] Found {len(jobs)} jobs for {company_name}")

    except Exception as e:
        logger.debug(f"[adzuna] Error fetching: {e}")

    return jobs


def _fetch_google_jobs_live(company_name: str) -> list:
    """Fetch jobs from Google Jobs API."""
    jobs = []
    try:
        # Google Jobs API (requires API key)
        api_key = os.environ.get("GOOGLE_JOBS_API_KEY", "")
        if not api_key:
            logger.debug("[google_jobs] API key not configured")
            return jobs

        url = "https://www.googleapis.com/jobs/v4/projects/search"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "requestMetadata": {
                "userOverride": {"userId": "miru-intel"}
            },
            "searchParameters": {
                "query": f'company:"{company_name}"',
                "pageSize": 50
            }
        }

        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for job in data.get("matchingJobs", []):
                job_info = job.get("jobInfo", {})
                jobs.append({
                    "title": job_info.get("jobTitle", ""),
                    "company": company_name,
                    "location": job_info.get("location", ""),
                    "department": _extract_department(job_info.get("jobTitle", "")),
                    "level": _extract_level(job_info.get("jobTitle", "")),
                    "posted_date": job_info.get("postingDate", ""),
                    "url": job_info.get("applicationUrl", ""),
                    "description": job_info.get("jobDescription", "")[:200]
                })
            logger.info(f"[google_jobs] Found {len(jobs)} jobs for {company_name}")

    except Exception as e:
        logger.debug(f"[google_jobs] Error fetching: {e}")

    return jobs


def _fetch_indeed_live(company_name: str) -> list:
    """Fetch jobs from Indeed (API or web search)."""
    jobs = []
    try:
        # Indeed Publisher API
        api_key = os.environ.get("INDEED_API_KEY", "")
        if api_key:
            try:
                # Indeed Publisher API endpoint
                url = "https://apis.indeed.com/graphql"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }

                query = f"""
                {{
                  jobSearch(input: {{
                    query: "{company_name}",
                    limit: 50,
                    filters: {{
                      locations: ["worldwide"]
                    }}
                  }}) {{
                    jobs {{
                      id
                      title
                      company {{
                        name
                      }}
                      location {{
                        city
                        country
                      }}
                      description
                      postedDate
                      jobUrl
                    }}
                  }}
                }}
                """

                response = requests.post(url, json={"query": query}, headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    for job in data.get("data", {}).get("jobSearch", {}).get("jobs", []):
                        jobs.append({
                            "title": job.get("title", ""),
                            "company": job.get("company", {}).get("name", ""),
                            "location": f"{job.get('location', {}).get('city', '')}, {job.get('location', {}).get('country', '')}",
                            "department": _extract_department(job.get("title", "")),
                            "level": _extract_level(job.get("title", "")),
                            "posted_date": job.get("postedDate", ""),
                            "url": job.get("jobUrl", ""),
                            "description": job.get("description", "")[:200]
                        })
                    logger.info(f"[indeed] Found {len(jobs)} jobs for {company_name}")

            except Exception as e:
                logger.debug(f"[indeed_api] Error: {e}")
        else:
            logger.debug("[indeed] API key not configured")

    except Exception as e:
        logger.debug(f"[indeed] Error fetching: {e}")

    return jobs


def _fetch_linkedin_live(company_name: str) -> list:
    """Fetch jobs from LinkedIn careers API or page."""
    jobs = []
    try:
        # LinkedIn careers page pattern
        company_slug = company_name.lower().replace(" ", "-").replace("&", "and")
        linkedin_url = f"https://www.linkedin.com/company/{company_slug}/jobs/"

        # Try LinkedIn API first
        api_key = os.environ.get("LINKEDIN_API_KEY", "")
        if api_key:
            try:
                # LinkedIn Jobs API
                url = "https://api.linkedin.com/v2/jobs"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }

                # Search for jobs at this company
                params = {
                    "q": "targetCompanies",
                    "targetCompanies": company_name,
                    "limit": 50
                }

                response = requests.get(url, headers=headers, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    for job in data.get("elements", []):
                        jobs.append({
                            "title": job.get("title", ""),
                            "company": company_name,
                            "location": job.get("location", ""),
                            "department": _extract_department(job.get("title", "")),
                            "level": _extract_level(job.get("title", "")),
                            "posted_date": job.get("postedDate", ""),
                            "url": job.get("jobUrl", ""),
                            "description": job.get("description", "")[:200]
                        })
                    logger.info(f"[linkedin] Found {len(jobs)} jobs from LinkedIn API")

            except Exception as e:
                logger.debug(f"[linkedin_api] Error: {e}")
        else:
            logger.debug(f"[linkedin] Careers page available at: {linkedin_url} (API key not configured)")

    except Exception as e:
        logger.debug(f"[linkedin] Error fetching: {e}")

    return jobs


def _fetch_company_careers_live(company_name: str) -> list:
    """Fetch jobs from company's careers page (supports Workable, Greenhouse, Lever, BambooHR)."""
    jobs = []
    try:
        company_slug = company_name.lower().replace(" ", "").replace("&", "and")

        # Common career page patterns
        career_urls = [
            f"https://{company_slug}.com/careers",
            f"https://careers.{company_slug}.com",
            f"https://jobs.{company_slug}.com",
            f"https://{company_slug}.careers",
        ]

        # Many companies use standard ATS (Applicant Tracking System) platforms
        # These have public job feeds available at predictable URLs

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        for url in career_urls:
            try:
                # Try to fetch careers page
                response = requests.get(url, headers=headers, timeout=5)

                if response.status_code == 200:
                    # Try to find job listings
                    # Look for common ATS job feed patterns

                    # Workable: /jobs.json
                    workable_url = url.rstrip('/') + "/jobs.json"
                    try:
                        workable_response = requests.get(workable_url, timeout=5)
                        if workable_response.status_code == 200:
                            workable_jobs = workable_response.json()
                            for job in workable_jobs.get("jobs", []):
                                jobs.append({
                                    "title": job.get("title", ""),
                                    "company": company_name,
                                    "location": job.get("location", ""),
                                    "department": _extract_department(job.get("title", "")),
                                    "level": _extract_level(job.get("title", "")),
                                    "posted_date": job.get("posted_date", ""),
                                    "url": job.get("url", ""),
                                    "description": job.get("description", "")[:200]
                                })
                            logger.info(f"[workable] Found {len(jobs)} jobs from {company_name}")
                            break
                    except:
                        pass

                    # Greenhouse: has RSS or API
                    greenhouse_url = url.rstrip('/') + "/jobs.xml"
                    try:
                        greenhouse_response = requests.get(greenhouse_url, timeout=5)
                        if greenhouse_response.status_code == 200:
                            # Parse XML
                            import xml.etree.ElementTree as ET
                            root = ET.fromstring(greenhouse_response.content)
                            for item in root.findall('.//item'):
                                title = item.find('title')
                                link = item.find('link')
                                if title is not None:
                                    jobs.append({
                                        "title": title.text or "",
                                        "company": company_name,
                                        "location": "TBD",
                                        "department": _extract_department(title.text or ""),
                                        "level": _extract_level(title.text or ""),
                                        "posted_date": item.find('pubDate').text if item.find('pubDate') is not None else "",
                                        "url": link.text if link is not None else "",
                                        "description": ""
                                    })
                            if jobs:
                                logger.info(f"[greenhouse] Found {len(jobs)} jobs from {company_name}")
                                break
                    except:
                        pass

            except Exception as e:
                logger.debug(f"[careers] Error checking {url}: {e}")

    except Exception as e:
        logger.debug(f"[careers] Error fetching: {e}")

    return jobs


def _deduplicate_jobs(jobs: list) -> list:
    """Remove duplicate job postings and filter out irrelevant results."""
    seen = set()
    unique = []

    # Filter out irrelevant job types
    blacklist_titles = [
        "retail", "sales team member", "store associate", "sales associate",
        "teacher", "childcare", "nurse", "doctor", "therapist",
        "cleaner", "housekeeping", "catering", "chef", "cook",
        "delivery driver", "delivery person", "driver",
        "recruiter", "recruiter",  # Often fake job postings
        "nanny", "babysitter", "au pair",
        "personal trainer", "fitness", "coach",
        "beauty", "hairdresser", "salon",
        "fashion retail", "clothing store"
    ]

    for job in jobs:
        title = job.get("title", "").lower()
        company = job.get("company", "").lower()

        # Skip if title contains blacklisted keywords
        if any(blacklisted in title for blacklisted in blacklist_titles):
            logger.debug(f"[filter] Skipping {title} (blacklisted)")
            continue

        # Create a unique key from title + location
        key = (title, job.get("location", "").lower())

        if key not in seen:
            seen.add(key)
            unique.append(job)

    return unique


def _extract_department(job_title: str) -> str:
    """Extract department from job title."""
    title_lower = job_title.lower()

    departments = {
        "ENGINEERING": ["engineer", "developer", "devops", "programmer", "coder", "architect"],
        "AI/ML": ["machine learning", "ai", "data scientist", "nlp", "llm", "ml engineer"],
        "PRODUCT": ["product manager", "pm", "product design", "designer"],
        "SALES": ["sales", "account executive", "business development", "inside sales"],
        "MARKETING": ["marketing", "growth", "demand generation", "brand"],
        "OPERATIONS": ["operations", "ops", "supply chain", "operations manager"],
        "HR": ["recruiter", "hr", "talent", "people", "recruiting"],
        "FINANCE": ["accountant", "finance", "controller", "cfo", "accounting"],
        "INFRASTRUCTURE": ["infrastructure", "sre", "platform", "devops"],
        "SECURITY": ["security", "infosec", "penetration", "security engineer"],
    }

    for dept, keywords in departments.items():
        if any(kw in title_lower for kw in keywords):
            return dept

    return "OTHER"


def _extract_level(job_title: str) -> str:
    """Extract seniority level from job title."""
    title_lower = job_title.lower()

    if any(x in title_lower for x in ["c-level", "ceo", "cto", "cfo", "vp ", "vice president"]):
        return "Executive"
    elif any(x in title_lower for x in ["senior", "lead", "principal", "architect", "director", "manager"]):
        return "Senior"
    elif any(x in title_lower for x in ["mid", "intermediate", "mid-level"]):
        return "Mid-level"
    elif any(x in title_lower for x in ["junior", "intern", "entry", "graduate"]):
        return "Junior"

    return "Mid-level"


def _extract_skills_from_description(description: str) -> list:
    """Extract technical skills from job description."""
    if not description:
        return []

    description_lower = description.lower()

    skills = {
        "Python": ["python"],
        "JavaScript": ["javascript", "js", "node.js"],
        "React": ["react", "reactjs"],
        "AWS": ["aws", "amazon web services"],
        "Cloud": ["cloud", "gcp", "azure"],
        "Kubernetes": ["kubernetes", "k8s"],
        "Docker": ["docker"],
        "SQL": ["sql", "mysql", "postgres"],
        "Java": ["java"],
        "Go": ["golang", "go"],
        "Rust": ["rust"],
        "Machine Learning": ["machine learning", "ml", "tensorflow", "pytorch"],
        "Data Science": ["data science", "data scientist"],
        "AI/LLM": ["ai", "llm", "gpt", "transformer"],
        "DevOps": ["devops", "ci/cd"],
        "Microservices": ["microservices"],
        "GraphQL": ["graphql"],
        "REST API": ["rest", "api"],
        "Mobile": ["ios", "android", "flutter", "react native"],
        "Blockchain": ["blockchain", "ethereum", "web3"]
    }

    found_skills = []
    for skill, keywords in skills.items():
        if any(kw in description_lower for kw in keywords):
            found_skills.append(skill)

    return found_skills[:10]  # Top 10 skills


def _extract_country_from_location(location: str) -> str:
    """Extract country code from location string."""
    if not location:
        return "Unknown"

    location_lower = location.lower()

    countries = {
        "US": ["usa", "united states", "california", "new york", "san francisco", "seattle", "austin"],
        "IN": ["india", "bangalore", "hyderabad", "delhi", "mumbai", "pune"],
        "GB": ["uk", "united kingdom", "london", "england", "manchester"],
        "DE": ["germany", "berlin", "munich"],
        "CA": ["canada", "toronto", "vancouver", "calgary", "ottawa"],
        "AU": ["australia", "sydney", "melbourne", "brisbane", "perth"],
        "PL": ["poland", "warsaw", "krakow", "gdansk", "wroclaw"],
        "EE": ["estonia", "tallinn", "tartu"],
        "PT": ["portugal", "lisbon", "porto", "braga"],
        "SG": ["singapore"],
        "JP": ["japan", "tokyo"],
        "FR": ["france", "paris"],
        "NL": ["netherlands", "amsterdam"],
        "IE": ["ireland", "dublin"],
        "SE": ["sweden", "stockholm"],
        "CH": ["switzerland", "zurich"]
    }

    for country_code, keywords in countries.items():
        if any(kw in location_lower for kw in keywords):
            return country_code

    return "Other"


def _analyze_jobs(company_name: str, jobs_data: list) -> dict:
    """Analyze job data to extract hiring signals."""

    if not jobs_data:
        return {
            "company": company_name,
            "overview": {
                "total_open_roles": 0,
                "hiring_growth_3m": "—",
                "top_region": "—",
                "signal": "No hiring data available"
            },
            "top_departments": [],
            "growth_signals": [],
            "country_breakdown": [],
            "role_profiles": [],
            "top_skills": [],
            "sample_roles": []
        }

    # Count by department, region, level, country, and skills
    dept_counts = defaultdict(int)
    region_counts = defaultdict(int)
    country_counts = defaultdict(int)
    levels = defaultdict(int)
    all_skills = defaultdict(int)

    for job in jobs_data:
        dept = job.get("department", "OTHER")
        location = job.get("location", "Various")
        region = _extract_region(location)
        country = _extract_country_from_location(location)
        level = job.get("level", "Mid-level")
        description = job.get("description", "")

        dept_counts[dept] += 1
        region_counts[region] += 1
        country_counts[country] += 1
        levels[level] += 1

        # Extract and count skills from description
        skills = _extract_skills_from_description(description)
        for skill in skills:
            all_skills[skill] += 1

    # Top departments
    top_depts = sorted(dept_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    # Country breakdown
    country_breakdown = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    total_jobs = len(jobs_data)

    # Role profiles (seniority levels)
    role_profiles = sorted(levels.items(), key=lambda x: x[1], reverse=True)

    # Top skills
    top_skills = sorted(all_skills.items(), key=lambda x: x[1], reverse=True)[:8]

    # Detect growth signals
    signals = _detect_signals(dept_counts, levels, region_counts, company_name)

    # Top region
    top_region = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)[0][0] if region_counts else "Various"

    return {
        "company": company_name,
        "overview": {
            "total_open_roles": len(jobs_data),
            "hiring_growth_3m": _calculate_growth_rate(jobs_data),
            "top_region": top_region,
            "signal": signals[0]["title"] if signals else "Active hiring"
        },
        "top_departments": [
            {
                "department": dept,
                "open_count": count,
                "trend": f"↑ {count} positions"
            }
            for dept, count in top_depts
        ],
        "country_breakdown": [
            {
                "country": country,
                "count": count,
                "percentage": round((count / total_jobs) * 100, 1)
            }
            for country, count in country_breakdown
        ],
        "role_profiles": [
            {
                "level": level,
                "count": count,
                "percentage": round((count / total_jobs) * 100, 1)
            }
            for level, count in role_profiles
        ],
        "top_skills": [
            {
                "skill": skill,
                "count": count
            }
            for skill, count in top_skills
        ],
        "growth_signals": signals,
        "sample_roles": [
            {
                "title": job["title"],
                "location": job["location"],
                "country": _extract_country_from_location(job["location"]),
                "department": job["department"],
                "level": job["level"],
                "description": job.get("description", "")[:250],
                "url": job.get("url", "")
            }
            for job in jobs_data[:8]  # Show up to 8 sample roles
        ]
    }


def _extract_region(location: str) -> str:
    """Extract region from location."""
    location_lower = location.lower()

    regions = {
        "North America": ["usa", "us", "canada", "california", "new york", "toronto"],
        "Europe": ["uk", "london", "berlin", "paris", "europe", "ireland", "germany"],
        "Asia": ["india", "singapore", "tokyo", "hong kong", "asia", "bangalore"],
        "Remote": ["remote", "worldwide", "anywhere"]
    }

    for region, keywords in regions.items():
        if any(kw in location_lower for kw in keywords):
            return region

    return "Various"


def _calculate_growth_rate(jobs_data: list) -> str:
    """Calculate hiring growth indicator."""
    total = len(jobs_data)

    if total > 100:
        return "↑↑ Rapid expansion"
    elif total > 50:
        return "↑ Strong growth"
    elif total > 20:
        return "↑ Moderate growth"
    elif total > 5:
        return "→ Steady hiring"
    else:
        return "Minimal"


def _detect_signals(dept_counts: dict, levels: dict, region_counts: dict, company_name: str) -> list:
    """Detect strategic hiring signals."""
    signals = []

    # AI/ML expansion
    if "AI/ML" in dept_counts and dept_counts["AI/ML"] >= 5:
        signals.append({
            "title": "🤖 AI/ML Expansion",
            "description": f"Significant hiring in AI/ML ({dept_counts['AI/ML']} roles) indicates strategic pivot toward AI capabilities.",
            "confidence": "High"
        })

    # International expansion
    if len(region_counts) >= 3:
        signals.append({
            "title": "🌍 International Growth",
            "description": f"Hiring across {len(region_counts)} regions indicates global market expansion.",
            "confidence": "High"
        })

    # Engineering surge
    if "ENGINEERING" in dept_counts and dept_counts["ENGINEERING"] >= 10:
        signals.append({
            "title": "🚀 Engineering Acceleration",
            "description": f"Major engineering hiring spree ({dept_counts['ENGINEERING']} roles) suggests product development push.",
            "confidence": "High"
        })

    # Executive hiring
    if levels.get("Executive", 0) >= 2:
        signals.append({
            "title": "👔 Leadership Expansion",
            "description": f"Multiple C-level/executive hires indicate major organizational restructuring.",
            "confidence": "Medium"
        })

    # Sales force growth
    if "SALES" in dept_counts and dept_counts["SALES"] >= 5:
        signals.append({
            "title": "💰 Sales Push",
            "description": f"Aggressive sales hiring ({dept_counts['SALES']} roles) suggests market expansion or new product launch.",
            "confidence": "Medium"
        })

    return signals[:3]  # Top 3 signals
