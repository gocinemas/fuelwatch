"""
Market Entry Scoring Engine
Calculates market readiness score for brand expansion decisions
"""

from typing import Dict, List, Tuple


class MarketEntryScorer:
    """Scores market attractiveness for brand expansion (0-10 scale)"""

    def __init__(self):
        """Initialize scoring weights and thresholds"""
        self.min_score = 0
        self.max_score = 10

    def score_market(
        self,
        brand_name: str,
        market_country: str,
        category: str,
        brand_data: Dict,
        market_data: Dict,
        competitive_data: Dict,
    ) -> Dict:
        """
        Calculate market entry score for a brand-market combination

        Args:
            brand_name: Brand name
            market_country: Target market (UK, USA, India, etc.)
            category: Product category (skincare, beverages, etc.)
            brand_data: Brand fundamentals (positioning tier, etc.)
            market_data: Market economics (growth, size, status)
            competitive_data: Competitive landscape (competitors, threat level)

        Returns:
            {
                "score": 7.2,
                "recommendation": "green|yellow|red",
                "recommendation_text": "High opportunity...",
                "factors": {
                    "market_growth": 8.0,
                    "affluence_match": 7.0,
                    "competition": 5.0,
                    "distribution_accessibility": 8.0,
                    "ppp_viability": 7.0,
                },
                "insights": ["Growing market", "Strong local competition"],
                "risks": ["High competitive intensity"],
            }
        """

        # Extract key metrics
        positioning_tier = brand_data.get("positioning_tier", "mass-market").lower()
        market_status = market_data.get("category_status", "mature")
        category_cagr = float(market_data.get("category_cagr_3yr", 0))
        market_size = float(market_data.get("category_market_size_usd_millions", 0))
        ppp_index = float(market_data.get("ppp_index", 1.0))
        competitive_intensity = competitive_data.get("competitive_intensity", "medium")
        num_competitors = len(competitive_data.get("direct_competitors", []))

        # Calculate factor scores
        factors = {}

        # 1. Market Growth Score (0-10)
        # High growth (>7% CAGR) = 8-10, Mature (2-5%) = 4-6, Saturated (<2%) = 0-3
        if category_cagr > 8:
            factors["market_growth"] = 9.0
        elif category_cagr > 5:
            factors["market_growth"] = 7.5
        elif category_cagr > 2:
            factors["market_growth"] = 5.0
        else:
            factors["market_growth"] = 2.0

        # 2. Affluence Match Score (0-10)
        # How well brand positioning matches market's affluence level
        affluence_score = self._calculate_affluence_match(
            positioning_tier, market_country, market_data
        )
        factors["affluence_match"] = affluence_score

        # 3. Competitive Threat Score (0-10, inverted: lower threat = higher score)
        # Few competitors in niche = 8-10, medium competition = 5-7, saturated = 0-3
        if competitive_intensity == "low":
            factors["competition"] = 8.0 - (num_competitors * 0.5)
        elif competitive_intensity == "medium":
            factors["competition"] = 6.0 - (num_competitors * 0.3)
        else:  # high
            factors["competition"] = 3.0 - (num_competitors * 0.2)

        factors["competition"] = max(0, min(10, factors["competition"]))

        # 4. Distribution Accessibility (0-10)
        # Mass-market brands can access more channels = higher score
        dist_strategy = brand_data.get("distribution_strategy", "mass_market").lower()
        if dist_strategy == "mass_market":
            factors["distribution_accessibility"] = 8.5
        elif dist_strategy == "selective":
            factors["distribution_accessibility"] = 6.0
        else:  # exclusive
            factors["distribution_accessibility"] = 4.0

        # Boost if market has good infrastructure (UK/USA)
        if market_country in ["UK", "USA"]:
            factors["distribution_accessibility"] += 1.0

        factors["distribution_accessibility"] = min(10, factors["distribution_accessibility"])

        # 5. PPP Viability (0-10)
        # Can the brand's positioning work at this PPP level?
        ppp_score = self._calculate_ppp_viability(
            positioning_tier, ppp_index, category_cagr
        )
        factors["ppp_viability"] = ppp_score

        # Calculate Brand Strength Score (viability NOW)
        # Factors: Can we operate here? How healthy would the brand be?
        strength_weights = {
            "distribution_accessibility": 0.30,  # Can we reach customers?
            "affluence_match": 0.30,  # Do customers fit our positioning?
            "ppp_viability": 0.25,  # Can we afford to be here?
            "competition": 0.15,  # Can we compete?
        }

        brand_strength = sum(factors[key] * strength_weights[key] for key in strength_weights)
        brand_strength = round(brand_strength, 1)

        # Calculate Growth Opportunity Score (upside potential)
        # Factors: Should we invest/expand here? How much upside?
        # NOTE: market_size is category size (all skincare in market), not individual brand size
        market_size_factor = min(10, (market_size / 5000) * 10)  # Normalize market size

        # Market status: emerging > high_growth > mature
        # BUT: Mature market leaders (strong brand + stable market) = good opportunity
        market_status_score = 2.0
        if market_status == "high_growth":
            market_status_score = 8.0
        elif market_status == "emerging":
            market_status_score = 7.0
        elif market_status == "mature":
            # Mature market: if brand is strong, it's a stable/profitable opportunity
            # Don't penalize market leaders for being in mature markets
            if brand_strength >= 7.0:
                market_status_score = 7.0  # Leadership in mature market = good
            else:
                market_status_score = 4.0  # Weak position in mature market = risky

        growth_weights = {
            "market_growth": 0.40,  # Most important: CAGR
            "market_size": 0.25,  # Larger market = more opportunity (use normalized value)
            "competition": 0.20,  # Lower competition = more upside
            "market_status": 0.15,  # Status indicates stage
        }

        growth_factors = {
            "market_growth": factors["market_growth"],
            "market_size": market_size_factor,
            "competition": factors["competition"],
            "market_status": market_status_score,
        }

        growth_opportunity = sum(growth_factors[key] * growth_weights[key] for key in growth_weights)
        growth_opportunity = round(growth_opportunity, 1)

        # Generate recommendation based on BOTH scores
        # Market leaders (7.0+ strength) in mature markets get credit for stability
        if brand_strength >= 7.0 and growth_opportunity >= 7.0:
            recommendation = "green"
            recommendation_text = "🟢 Strong & Growing: Healthy brand + high-growth market. Invest aggressively."
        elif brand_strength >= 7.0 and market_status == "mature":
            # Market leaders in mature markets (stable, profitable)
            recommendation = "green"
            recommendation_text = "🟢 Market Leader: Dominant brand in stable market. Maintain & optimize for profitability."
        elif brand_strength >= 7.0 and growth_opportunity >= 5.0:
            recommendation = "green"
            recommendation_text = "🟢 Strong Foundation: Well-positioned brand. Optimize & maintain."
        elif brand_strength >= 5.0 and growth_opportunity >= 7.0:
            recommendation = "yellow"
            recommendation_text = "🟡 Growth Opportunity: Market is attractive, brand needs work. Requires strategic positioning."
        elif brand_strength >= 7.0:
            # Strong brand with moderate growth (common for mature market leaders)
            recommendation = "yellow"
            recommendation_text = "🟡 Stable Leader: Strong position but limited growth. Focus on retention & profitability."
        elif brand_strength >= 5.0 or growth_opportunity >= 5.0:
            recommendation = "yellow"
            recommendation_text = "🟡 Conditional: Either moderate viability or limited growth. Requires differentiation."
        else:
            recommendation = "red"
            recommendation_text = "🔴 Not Recommended: Weak brand position + limited growth. Market challenges outweigh opportunities."

        # Generate insights
        insights = self._generate_insights(
            market_country, market_status, category_cagr, positioning_tier, factors
        )

        # Generate risk flags
        risks = self._generate_risks(
            market_country, competitive_intensity, num_competitors, ppp_index, factors
        )

        return {
            "brand_strength_score": brand_strength,
            "growth_opportunity_score": growth_opportunity,
            "recommendation": recommendation,
            "recommendation_text": recommendation_text,
            "brand_strength_factors": {
                "distribution": factors["distribution_accessibility"],
                "affluence": factors["affluence_match"],
                "ppp_viability": factors["ppp_viability"],
                "competition": factors["competition"],
            },
            "growth_opportunity_factors": {
                "market_growth": factors["market_growth"],
                "market_size": market_size_factor,
                "competition": factors["competition"],
                "market_status": market_status_score,
            },
            "insights": insights,
            "risks": risks,
            "market_opportunity": self._calculate_revenue_potential(market_size, market_country, positioning_tier),
        }

    def _calculate_affluence_match(
        self, positioning_tier: str, market_country: str, market_data: Dict
    ) -> float:
        """Score how well brand positioning matches market's affluence level"""

        # Market affluence levels
        affluence_levels = {
            "UK": "high",
            "USA": "high",
            "Germany": "high",
            "Japan": "high",
            "India": "low",
            "Brazil": "medium",
            "Indonesia": "low",
        }

        affluent_pop = float(market_data.get("affluent_consumers_millions", 0))
        total_pop = float(
            market_data.get("urban_population_millions", affluent_pop + 50)
        )

        if total_pop == 0:
            affluent_pct = 0.2
        else:
            affluent_pct = affluent_pop / total_pop

        market_affluence = affluence_levels.get(market_country, "medium")

        # Match brand tier to market affluence
        tier_match_scores = {
            "economy": {
                "high": 6.0,  # Economy brands struggle in affluent markets
                "medium": 8.0,
                "low": 9.0,  # Perfect match
            },
            "mass-market": {
                "high": 7.5,  # Good fit
                "medium": 9.0,  # Excellent fit
                "low": 8.5,
            },
            "mass-prestige": {
                "high": 8.5,
                "medium": 7.5,
                "low": 5.0,  # Harder in emerging markets
            },
            "premium": {
                "high": 9.0,  # Great in affluent markets
                "medium": 6.0,
                "low": 3.0,  # Very difficult
            },
            "luxury": {
                "high": 8.5,  # Strong in affluent markets
                "medium": 4.0,
                "low": 1.0,  # Nearly impossible
            },
        }

        base_score = tier_match_scores.get(positioning_tier, {}).get(market_affluence, 5.0)

        # Adjust based on affluent population size
        if affluent_pct > 0.3:
            adjustment = 1.0  # Boost if many affluent consumers
        elif affluent_pct > 0.15:
            adjustment = 0.5
        else:
            adjustment = -1.0

        return min(10, max(0, base_score + adjustment))

    def _calculate_ppp_viability(
        self, positioning_tier: str, ppp_index: float, category_cagr: float
    ) -> float:
        """Score PPP viability: can this brand's positioning work at this PPP level?"""

        # Higher PPP (closer to 1.0) = more viable for premium brands
        # Lower PPP (closer to 0) = need affordable positioning

        if ppp_index >= 0.9:  # Developed market (UK, USA)
            # Any tier works, but high-growth emerging markets score lower
            if category_cagr > 7:
                return 9.0  # Mature market, any brand fine
            else:
                return 7.0

        elif ppp_index >= 0.4:  # Middle-income (Brazil, China)
            # Mid-tier works best
            if positioning_tier in ["mass-market", "mass-prestige"]:
                return 8.5
            elif positioning_tier == "economy":
                return 7.0
            else:
                return 5.0

        else:  # Low-income (India, Indonesia)
            # Need affordable positioning, but growing middle class
            if positioning_tier == "economy":
                return 6.0
            elif positioning_tier == "mass-market":
                return 7.5  # "Affordable premium" works here
            elif positioning_tier == "mass-prestige":
                return 8.0  # Growing market, aspirational tier works
            else:  # premium/luxury
                return 3.0  # Only for ultra-rich segment

    def _generate_insights(
        self,
        market_country: str,
        market_status: str,
        category_cagr: float,
        positioning_tier: str,
        factors: Dict,
    ) -> List[str]:
        """Generate actionable insights"""

        insights = []

        # Growth insights
        if category_cagr > 8:
            insights.append("📈 Fast-growing market - strong demand tailwinds")
        elif category_cagr > 5:
            insights.append("📊 Healthy category growth - above GDP growth")
        elif category_cagr < 2:
            insights.append("⚠️ Mature/saturated market - limited growth")

        # Affluence insights
        if factors["affluence_match"] >= 8:
            insights.append("💰 Excellent market-brand fit for target affluence level")
        elif factors["affluence_match"] < 5:
            insights.append("⚠️ Market affluence mismatch - may require positioning shift")

        # Competition insights
        if factors["competition"] < 4:
            insights.append("🏆 Highly competitive market - need differentiation")
        elif factors["competition"] > 7:
            insights.append("🎯 Low competition - strong opportunity for market leadership")

        # Distribution insights
        if market_country in ["UK", "USA"]:
            insights.append("✅ Strong retail infrastructure - distribution accessible")
        elif market_country == "India":
            insights.append("📱 Growing e-commerce - digital-first distribution possible")

        return insights

    def _generate_risks(
        self,
        market_country: str,
        competitive_intensity: str,
        num_competitors: int,
        ppp_index: float,
        factors: Dict,
    ) -> List[str]:
        """Generate risk flags"""

        risks = []

        if competitive_intensity == "high":
            risks.append(f"🔴 High competitive intensity ({num_competitors}+ major competitors)")

        if factors["affluence_match"] < 4:
            risks.append("💔 Poor affluence-positioning match - may struggle with target segment")

        if ppp_index < 0.25:
            risks.append("💸 Very low purchasing power - aggressive pricing needed")

        if market_country in ["India", "Indonesia", "Brazil"]:
            risks.append("🌍 Emerging market dynamics - regulatory/currency volatility")

        if factors["distribution_accessibility"] < 5:
            risks.append("📦 Limited distribution channels - may require direct-to-consumer strategy")

        return risks

    def _calculate_revenue_potential(
        self, market_size_usd_millions: float, market_country: str, positioning_tier: str
    ) -> Dict:
        """Estimate revenue potential at different market share levels"""

        # Realistic market share targets by positioning tier
        share_targets = {
            "economy": {"low": 5, "medium": 10, "high": 15},  # % market share
            "mass-market": {"low": 3, "medium": 7, "high": 12},
            "mass-prestige": {"low": 2, "medium": 5, "high": 10},
            "premium": {"low": 1, "medium": 3, "high": 7},
            "luxury": {"low": 0.5, "medium": 1.5, "high": 3},
        }

        targets = share_targets.get(positioning_tier, {"low": 2, "medium": 5, "high": 10})

        return {
            "at_1_pct_share": round(market_size_usd_millions * 0.01),
            "at_target_share": round(market_size_usd_millions * (targets["medium"] / 100)),
            "at_high_share": round(market_size_usd_millions * (targets["high"] / 100)),
            "usd_millions": market_size_usd_millions,
        }


# Example usage
if __name__ == "__main__":
    scorer = MarketEntryScorer()

    # Example: Dove skincare expansion to India
    result = scorer.score_market(
        brand_name="Dove",
        market_country="India",
        category="skincare",
        brand_data={
            "positioning_tier": "mass-market",
            "distribution_strategy": "mass_market",
        },
        market_data={
            "category_status": "high_growth",
            "category_cagr_3yr": 8.2,
            "category_market_size_usd_millions": 2100,
            "ppp_index": 0.25,
            "affluent_consumers_millions": 25,
            "urban_population_millions": 520,
        },
        competitive_data={
            "competitive_intensity": "medium",
            "direct_competitors": ["Himalaya", "Nykaa", "L'Oréal"],
        },
    )

    print("Market Entry Score: Dove in India")
    print(f"Score: {result['score']}/10")
    print(f"Recommendation: {result['recommendation_text']}")
    print(f"\nFactors:")
    for factor, score in result['factors'].items():
        print(f"  {factor}: {score}/10")
    print(f"\nInsights:")
    for insight in result['insights']:
        print(f"  {insight}")
    print(f"\nRisks:")
    for risk in result['risks']:
        print(f"  {risk}")
