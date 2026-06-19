"""
Currency Conversion Service
Handles all currency conversions with consistent exchange rates
"""

# Exchange rates to USD (as of 2024)
EXCHANGE_RATES = {
    "GBP": 1.27,  # £1 = $1.27 USD
    "USD": 1.00,  # $1 = $1 USD
    "INR": 0.012, # ₹1 = $0.012 USD (1 USD = ~83 INR)
    "BRL": 0.20,  # Brazilian Real
    "IDR": 0.000063, # Indonesian Rupiah
}

# PPP Indices (World Bank reference, USA = 1.0)
PPP_INDICES = {
    "UK": 1.0,
    "USA": 1.0,
    "India": 0.25,
    "Brazil": 0.45,
    "Indonesia": 0.18,
}

# Currency by country
CURRENCY_BY_COUNTRY = {
    "UK": "GBP",
    "USA": "USD",
    "India": "INR",
    "Brazil": "BRL",
    "Indonesia": "IDR",
}


def get_exchange_rate(from_currency: str, to_currency: str = "USD") -> float:
    """Get exchange rate from one currency to another"""
    if from_currency not in EXCHANGE_RATES or to_currency not in EXCHANGE_RATES:
        raise ValueError(f"Unsupported currency: {from_currency} or {to_currency}")

    from_rate = EXCHANGE_RATES[from_currency]
    to_rate = EXCHANGE_RATES[to_currency]

    return to_rate / from_rate


def convert_to_usd(amount: float, currency: str) -> float:
    """Convert any currency amount to USD"""
    if currency not in EXCHANGE_RATES:
        raise ValueError(f"Unsupported currency: {currency}")

    return amount * EXCHANGE_RATES[currency]


def convert_from_usd(amount_usd: float, currency: str) -> float:
    """Convert USD to any currency"""
    if currency not in EXCHANGE_RATES:
        raise ValueError(f"Unsupported currency: {currency}")

    return amount_usd / EXCHANGE_RATES[currency]


def convert_currency(amount: float, from_currency: str, to_currency: str = "USD") -> float:
    """Convert between any two currencies"""
    usd_amount = convert_to_usd(amount, from_currency)
    return convert_from_usd(usd_amount, to_currency)


def get_ppp_index(country: str) -> float:
    """Get PPP index for a country"""
    if country not in PPP_INDICES:
        raise ValueError(f"Unsupported country: {country}")
    return PPP_INDICES[country]


def get_currency_for_country(country: str) -> str:
    """Get currency code for a country"""
    if country not in CURRENCY_BY_COUNTRY:
        raise ValueError(f"Unsupported country: {country}")
    return CURRENCY_BY_COUNTRY[country]


def format_price(amount: float, currency: str, decimal_places: int = 2) -> str:
    """Format price with currency symbol"""
    symbols = {
        "GBP": "£",
        "USD": "$",
        "INR": "₹",
        "BRL": "R$",
        "IDR": "Rp",
    }
    symbol = symbols.get(currency, currency)
    return f"{symbol}{amount:.{decimal_places}f}"


def calculate_ppp_adjusted_price(
    local_price: float,
    local_currency: str,
    target_country: str,
) -> dict:
    """
    Calculate PPP-adjusted price for comparison across markets

    Returns:
        {
            "local_price": 4.50,
            "local_currency": "GBP",
            "local_ppp": 1.0,
            "usd_equivalent": 5.71,
            "ppp_index": 0.25,
            "purchasing_power_equivalent": 1.43,
            "explanation": "At India's PPP level, this would cost ₹119"
        }
    """

    # Convert local price to USD
    usd_price = convert_to_usd(local_price, local_currency)

    # Get PPP indices
    source_country = None
    for country, currency in CURRENCY_BY_COUNTRY.items():
        if currency == local_currency:
            source_country = country
            break

    if not source_country:
        source_country = "USA"  # Default

    source_ppp = get_ppp_index(source_country)
    target_ppp = get_ppp_index(target_country)

    # Calculate PPP-adjusted price
    ppp_adjusted_usd = usd_price * (target_ppp / source_ppp)

    # Convert to target currency
    target_currency = get_currency_for_country(target_country)
    target_local = convert_from_usd(ppp_adjusted_usd, target_currency)

    return {
        "local_price": round(local_price, 2),
        "local_currency": local_currency,
        "local_ppp": source_ppp,
        "usd_equivalent": round(usd_price, 2),
        "target_country": target_country,
        "target_currency": target_currency,
        "ppp_index": target_ppp,
        "purchasing_power_equivalent": round(target_local, 2),
        "explanation": f"PPP-adjusted: At {target_country}'s purchasing power level, this would cost {format_price(target_local, target_currency)}",
    }


# Example usage:
if __name__ == "__main__":
    print("=== Currency Conversion Service ===\n")

    # Test 1: Simple conversion
    print("Test 1: Convert £4.50 to USD")
    usd = convert_to_usd(4.50, "GBP")
    print(f"  £4.50 = ${usd:.2f}\n")

    # Test 2: PPP calculation
    print("Test 2: Dove UK (£4.50) PPP-adjusted for India")
    result = calculate_ppp_adjusted_price(4.50, "GBP", "India")
    print(f"  Local: {format_price(result['local_price'], result['local_currency'])}")
    print(f"  USD Equivalent: ${result['usd_equivalent']}")
    print(f"  PPP-adjusted price in India: {format_price(result['purchasing_power_equivalent'], result['target_currency'])}")
    print(f"  Explanation: {result['explanation']}\n")

    # Test 3: India to USD
    print("Test 3: Convert ₹120 to USD")
    usd = convert_to_usd(120, "INR")
    print(f"  ₹120 = ${usd:.2f}")
