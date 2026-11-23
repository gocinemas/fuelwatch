"""
Bin collection lookup for any UK postcode, using each council's own checker.

  GET /api/bins/lookup?postcode=KT16 0DA   -> council, and either the address list or the council's link
  GET /api/bins/dates?postcode=..&uprn=..  -> collection dates, where a council connector supports it

Steps:
  1. postcodes.io gives the council for the postcode (any UK postcode).
  2. If we have a connector for that council, use its own checker's steps.
  3. Otherwise return the council's own bin page link.
Results are cached for 24 hours. Nothing about the person is stored.
"""
import os
import re
import time
import uuid
from urllib.parse import quote

import requests
from flask import jsonify, request

CACHE_SECONDS = 24 * 3600
# Off by default. Only turned on after WhenIsBins agree to commercial use (see hello@whenisbins.com).
WIB_ENABLED = os.environ.get("BINS_WHENISBINS_ENABLED", "") == "1"
WIB_BASE = "https://whenisbins.com/v1"
WIB_AGENT = "factory-humanagency"
_cache = {}
UA = "Factory-bin-lookup/1.0 (humanagency.co)"


def _cached(key, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = fn()
    _cache[key] = (time.time(), value)
    return value


def resolve_council(postcode):
    pc = re.sub(r"\s+", "", postcode or "").upper()
    def fetch():
        r = requests.get(f"https://api.postcodes.io/postcodes/{pc}", timeout=10)
        if r.status_code != 200:
            return None
        res = r.json().get("result") or {}
        return {"name": res.get("admin_district"), "code": (res.get("codes") or {}).get("admin_district")}
    return _cached("council:" + pc, fetch)


# ── Connectors: one per council, using that council's own checker ──

def _runnymede_addresses(postcode):
    url = "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day"
    r = requests.get(url, params={"postcode": postcode, "submit": "Find"},
                     headers={"User-Agent": UA}, timeout=20)
    html = r.text
    start = html.find('id="address"')
    if start < 0:
        return []
    block = html[start:html.find("</select>", start)]
    opts = re.findall(r'<option\s+value="(\d+)"\s*>\s*([^<]+?)\s*</option>', block, flags=re.S)
    return [{"id": v, "label": re.sub(r"\s+", " ", l).strip()} for v, l in opts]


def _runnymede_dates(uprn):
    """Same request the council's page makes when an address is chosen."""
    from datetime import datetime
    url = "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day"
    r = requests.get(url, params={"address": uprn, "submit": "Find"},
                     headers={"User-Agent": UA}, timeout=20)
    html = r.text
    rows = re.findall(r"<tr>\s*<td>(?:<span[^>]*></span>)?\s*([^<]+?)\s*</td>\s*<td>\s*([^<]+?)\s*</td>", html, flags=re.S)
    collections = []
    for service, when in rows:
        m = re.search(r"(\d{1,2} \w+ \d{4})", when)
        if not m:
            continue
        try:
            iso = datetime.strptime(m.group(1), "%d %B %Y").date().isoformat()
        except ValueError:
            continue
        collections.append({"bin": service.strip(), "date": iso})
    return {"collections": collections,
            "source": "Runnymede Borough Council bin collection checker",
            "checked": datetime.now().date().isoformat()}


def _wib_addresses(postcode):
    r = requests.get(f"{WIB_BASE}/addresses", params={"postcode": postcode},
                     headers={"X-Agent": WIB_AGENT, "User-Agent": UA}, timeout=20)
    if r.status_code != 200:
        return None, None
    d = r.json()
    council = (d.get("council") or {}).get("name")
    addresses = [{"id": c["id"], "label": c["label"]} for c in (d.get("candidates") or [])]
    return council, addresses


def _wib_dates(postcode, property_id):
    """Creates one lookup, waits for it to finish, and returns collections with the council's source link."""
    from datetime import datetime
    headers = {"X-Agent": WIB_AGENT, "User-Agent": UA, "Idempotency-Key": str(uuid.uuid4())}
    r = requests.post(f"{WIB_BASE}/lookups", json={"postcode": postcode, "property_id": property_id},
                      headers=headers, timeout=30)
    if r.status_code not in (200, 201):
        return None
    lk = r.json()
    lookup_id = lk.get("id")
    deadline = time.time() + 120
    while lk.get("status") in ("queued", "running", "partial") and time.time() < deadline:
        time.sleep(5)
        g = requests.get(f"{WIB_BASE}/lookups/{lookup_id}", headers=headers, timeout=20)
        if g.status_code != 200:
            break
        lk = g.json()
    res = lk.get("result") or {}
    if lk.get("status") != "done" or not res:
        return None
    collections = []
    for c in res.get("collections", []):
        for d in c.get("dates", []):
            collections.append({"bin": c.get("name"), "date": d})
    collections.sort(key=lambda x: x["date"])
    council = res.get("council") or {}
    return {"collections": collections,
            "source": (council.get("name") or "Council") + " (via WhenIsBins)",
            "council_page": council.get("lookup_url") or "",  # fallback set in the route
            "checked": datetime.now().date().isoformat()}


CONNECTORS = {
    "E07000212": {  # Runnymede
        "name": "Runnymede",
        "addresses": _runnymede_addresses,
        "dates": _runnymede_dates,
        "page": "https://www.runnymede.gov.uk/homepage/150/check-your-bin-collection-day",
    },
}


# Council websites by GSS code (from GOV.UK local authority links). Used for links only.
COUNCIL_SITES = {
"E06000001": "https://hartlepool.gov.uk",
"E06000002": "https://middlesbrough.gov.uk",
"E06000003": "https://redcar-cleveland.gov.uk",
"E06000004": "https://stockton.gov.uk",
"E06000005": "https://darlington.gov.uk",
"E06000006": "https://www4.halton.gov.uk",
"E06000007": "https://warrington.gov.uk",
"E06000008": "https://blackburn.gov.uk",
"E06000009": "https://blackpool.gov.uk",
"E06000010": "https://hull.gov.uk",
"E06000011": "https://eastriding.gov.uk",
"E06000012": "https://nelincs.gov.uk",
"E06000013": "https://northlincs.gov.uk",
"E06000014": "https://york.gov.uk",
"E06000015": "https://derby.gov.uk",
"E06000016": "https://leicester.gov.uk",
"E06000017": "https://rutland.gov.uk",
"E06000018": "https://nottinghamcity.gov.uk",
"E06000019": "https://herefordshire.gov.uk",
"E06000020": "https://telford.gov.uk",
"E06000021": "https://stoke.gov.uk",
"E06000022": "https://bathnes.gov.uk",
"E06000023": "https://bristol.gov.uk",
"E06000024": "https://n-somerset.gov.uk",
"E06000025": "https://southglos.gov.uk",
"E06000026": "https://plymouth.gov.uk",
"E06000027": "https://torbay.gov.uk",
"E06000030": "https://swindon.gov.uk",
"E06000031": "https://peterborough.gov.uk",
"E06000032": "https://luton.gov.uk",
"E06000033": "https://southend.gov.uk",
"E06000034": "https://thurrock.gov.uk",
"E06000035": "https://medway.gov.uk",
"E06000036": "https://bracknell-forest.gov.uk",
"E06000037": "https://westberks.gov.uk",
"E06000038": "https://reading.gov.uk",
"E06000039": "https://slough.gov.uk",
"E06000040": "https://rbwm.gov.uk",
"E06000041": "https://wokingham.gov.uk",
"E06000042": "https://milton-keynes.gov.uk",
"E06000043": "https://brighton-hove.gov.uk",
"E06000044": "https://portsmouth.gov.uk",
"E06000045": "https://southampton.gov.uk",
"E06000046": "https://iow.gov.uk",
"E06000047": "https://durham.gov.uk",
"E06000049": "https://cheshireeast.gov.uk",
"E06000050": "https://cheshirewestandchester.gov.uk",
"E06000051": "https://shropshire.gov.uk",
"E06000052": "https://cornwall.gov.uk",
"E06000053": "https://scilly.gov.uk",
"E06000054": "https://wiltshire.gov.uk",
"E06000055": "https://bedford.gov.uk",
"E06000056": "https://centralbedfordshire.gov.uk",
"E06000057": "https://northumberland.gov.uk",
"E06000058": "https://bcpcouncil.gov.uk",
"E06000059": "https://dorsetcouncil.gov.uk",
"E06000060": "https://buckinghamshire.gov.uk",
"E06000061": "https://northnorthants.gov.uk",
"E06000062": "https://westnorthants.gov.uk",
"E06000063": "https://cumberland.gov.uk",
"E06000064": "https://westmorlandandfurness.gov.uk",
"E07000008": "https://cambridge.gov.uk",
"E07000009": "https://eastcambs.gov.uk",
"E07000010": "https://fenland.gov.uk",
"E07000011": "https://huntingdonshire.gov.uk",
"E07000012": "https://scambs.gov.uk",
"E07000032": "https://ambervalley.gov.uk",
"E07000033": "https://bolsover.gov.uk",
"E07000034": "https://chesterfield.gov.uk",
"E07000035": "https://derbyshiredales.gov.uk",
"E07000036": "https://erewash.gov.uk",
"E07000037": "https://highpeak.gov.uk",
"E07000038": "https://ne-derbyshire.gov.uk",
"E07000039": "https://southderbyshire.gov.uk",
"E07000040": "https://eastdevon.gov.uk",
"E07000041": "https://exeter.gov.uk",
"E07000042": "https://middevon.gov.uk",
"E07000043": "https://northdevon.gov.uk",
"E07000044": "https://southhams.gov.uk",
"E07000045": "https://teignbridge.gov.uk",
"E07000046": "https://torridge.gov.uk",
"E07000047": "https://westdevon.gov.uk",
"E07000061": "https://lewes-eastbourne.gov.uk",
"E07000062": "https://hastings.gov.uk",
"E07000063": "https://lewes-eastbourne.gov.uk",
"E07000064": "https://rother.gov.uk",
"E07000065": "https://wealden.gov.uk",
"E07000066": "https://basildon.gov.uk",
"E07000067": "https://braintree.gov.uk",
"E07000068": "https://brentwood.gov.uk",
"E07000069": "https://castlepoint.gov.uk",
"E07000070": "https://chelmsford.gov.uk",
"E07000071": "https://colchester.gov.uk",
"E07000072": "https://eppingforestdc.gov.uk",
"E07000073": "https://harlow.gov.uk",
"E07000074": "https://maldon.gov.uk",
"E07000075": "https://rochford.gov.uk",
"E07000076": "https://tendringdc.gov.uk",
"E07000077": "https://uttlesford.gov.uk",
"E07000078": "https://cheltenham.gov.uk",
"E07000079": "https://cotswold.gov.uk",
"E07000080": "https://fdean.gov.uk",
"E07000081": "https://gloucester.gov.uk",
"E07000082": "https://stroud.gov.uk",
"E07000083": "https://tewkesbury.gov.uk",
"E07000084": "https://basingstoke.gov.uk",
"E07000085": "https://easthants.gov.uk",
"E07000086": "https://eastleigh.gov.uk",
"E07000087": "https://fareham.gov.uk",
"E07000088": "https://gosport.gov.uk",
"E07000089": "https://hart.gov.uk",
"E07000090": "https://havant.gov.uk",
"E07000091": "https://newforest.gov.uk",
"E07000092": "https://rushmoor.gov.uk",
"E07000093": "https://testvalley.gov.uk",
"E07000094": "https://winchester.gov.uk",
"E07000095": "https://broxbourne.gov.uk",
"E07000096": "https://dacorum.gov.uk",
"E07000098": "https://hertsmere.gov.uk",
"E07000099": "https://north-herts.gov.uk",
"E07000102": "https://threerivers.gov.uk",
"E07000103": "https://watford.gov.uk",
"E07000105": "https://ashford.gov.uk",
"E07000106": "https://canterbury.gov.uk",
"E07000107": "https://dartford.gov.uk",
"E07000108": "https://dover.gov.uk",
"E07000109": "https://gravesham.gov.uk",
"E07000110": "https://maidstone.gov.uk",
"E07000111": "https://sevenoaks.gov.uk",
"E07000112": "https://folkestone-hythe.gov.uk",
"E07000113": "https://swale.gov.uk",
"E07000114": "https://thanet.gov.uk",
"E07000115": "https://tmbc.gov.uk",
"E07000116": "https://tunbridgewells.gov.uk",
"E07000117": "https://burnley.gov.uk",
"E07000118": "https://chorley.gov.uk",
"E07000119": "https://fylde.gov.uk",
"E07000120": "https://hyndburnbc.gov.uk",
"E07000121": "https://lancaster.gov.uk",
"E07000122": "https://pendle.gov.uk",
"E07000123": "https://preston.gov.uk",
"E07000124": "https://ribblevalley.gov.uk",
"E07000125": "https://rossendale.gov.uk",
"E07000126": "https://southribble.gov.uk",
"E07000127": "https://westlancs.gov.uk",
"E07000128": "https://wyre.gov.uk",
"E07000129": "https://blaby.gov.uk",
"E07000130": "https://charnwood.gov.uk",
"E07000131": "https://harborough.gov.uk",
"E07000132": "https://hinckley-bosworth.gov.uk",
"E07000133": "https://melton.gov.uk",
"E07000134": "https://nwleics.gov.uk",
"E07000135": "https://oadby-wigston.gov.uk",
"E07000136": "https://boston.gov.uk",
"E07000137": "https://e-lindsey.gov.uk",
"E07000138": "https://lincoln.gov.uk",
"E07000139": "https://n-kesteven.gov.uk",
"E07000140": "https://sholland.gov.uk",
"E07000141": "https://southkesteven.gov.uk",
"E07000142": "https://west-lindsey.gov.uk",
"E07000143": "https://breckland.gov.uk",
"E07000144": "https://southnorfolkandbroadland.gov.uk",
"E07000145": "https://great-yarmouth.gov.uk",
"E07000146": "https://west-norfolk.gov.uk",
"E07000147": "https://north-norfolk.gov.uk",
"E07000148": "https://norwich.gov.uk",
"E07000149": "https://southnorfolkandbroadland.gov.uk",
"E07000170": "https://ashfield.gov.uk",
"E07000171": "https://bassetlaw.gov.uk",
"E07000172": "https://broxtowe.gov.uk",
"E07000173": "https://gedling.gov.uk",
"E07000174": "https://mansfield.gov.uk",
"E07000175": "https://newark-sherwooddc.gov.uk",
"E07000176": "https://rushcliffe.gov.uk",
"E07000177": "https://cherwell.gov.uk",
"E07000178": "https://oxford.gov.uk",
"E07000179": "https://southoxon.gov.uk",
"E07000180": "https://whitehorsedc.gov.uk",
"E07000181": "https://westoxon.gov.uk",
"E07000192": "https://cannockchasedc.gov.uk",
"E07000193": "https://eaststaffsbc.gov.uk",
"E07000194": "https://lichfielddc.gov.uk",
"E07000195": "https://newcastle-staffs.gov.uk",
"E07000196": "https://sstaffs.gov.uk",
"E07000197": "https://staffordbc.gov.uk",
"E07000198": "https://staffsmoorlands.gov.uk",
"E07000199": "https://tamworth.gov.uk",
"E07000200": "https://babergh.gov.uk",
"E07000202": "https://ipswich.gov.uk",
"E07000203": "https://midsuffolk.gov.uk",
"E07000207": "https://elmbridge.gov.uk",
"E07000208": "https://epsom-ewell.gov.uk",
"E07000209": "https://guildford.gov.uk",
"E07000210": "https://molevalley.gov.uk",
"E07000211": "https://reigate-banstead.gov.uk",
"E07000212": "https://runnymede.gov.uk",
"E07000213": "https://spelthorne.gov.uk",
"E07000214": "https://surreyheath.gov.uk",
"E07000215": "https://tandridge.gov.uk",
"E07000216": "https://waverley.gov.uk",
"E07000217": "https://woking.gov.uk",
"E07000218": "https://northwarks.gov.uk",
"E07000219": "https://nuneatonandbedworth.gov.uk",
"E07000220": "https://rugby.gov.uk",
"E07000221": "https://stratford.gov.uk",
"E07000222": "https://warwickdc.gov.uk",
"E07000223": "https://adur-worthing.gov.uk",
"E07000224": "https://arun.gov.uk",
"E07000225": "https://chichester.gov.uk",
"E07000226": "https://crawley.gov.uk",
"E07000227": "https://horsham.gov.uk",
"E07000228": "https://midsussex.gov.uk",
"E07000229": "https://adur-worthing.gov.uk",
"E07000234": "https://bromsgrove.gov.uk",
"E07000235": "https://malvernhills.gov.uk",
"E07000236": "https://redditchbc.gov.uk",
"E07000237": "https://worcester.gov.uk",
"E07000238": "https://wychavon.gov.uk",
"E07000239": "https://wyreforestdc.gov.uk",
"E07000240": "https://stalbans.gov.uk",
"E07000241": "https://welhat.gov.uk",
"E07000242": "https://eastherts.gov.uk",
"E07000243": "https://stevenage.gov.uk",
"E07000244": "https://eastsuffolk.gov.uk",
"E07000245": "https://westsuffolk.gov.uk",
"E08000001": "https://bolton.gov.uk",
"E08000002": "https://bury.gov.uk",
"E08000003": "https://manchester.gov.uk",
"E08000004": "https://oldham.gov.uk",
"E08000005": "https://rochdale.gov.uk",
"E08000006": "https://salford.gov.uk",
"E08000007": "https://stockport.gov.uk",
"E08000008": "https://tameside.gov.uk",
"E08000009": "https://trafford.gov.uk",
"E08000010": "https://wigan.gov.uk",
"E08000011": "https://knowsley.gov.uk",
"E08000012": "https://liverpool.gov.uk",
"E08000013": "https://sthelens.gov.uk",
"E08000014": "https://sefton.gov.uk",
"E08000015": "https://wirral.gov.uk",
"E08000017": "https://doncaster.gov.uk",
"E08000018": "https://rotherham.gov.uk",
"E08000021": "https://newcastle.gov.uk",
"E08000022": "https://my.northtyneside.gov.uk",
"E08000023": "https://southtyneside.gov.uk",
"E08000024": "https://sunderland.gov.uk",
"E08000025": "https://birmingham.gov.uk",
"E08000026": "https://coventry.gov.uk",
"E08000027": "https://dudley.gov.uk",
"E08000028": "https://sandwell.gov.uk",
"E08000029": "https://solihull.gov.uk",
"E08000030": "https://go.walsall.gov.uk",
"E08000031": "https://wolverhampton.gov.uk",
"E08000032": "https://bradford.gov.uk",
"E08000033": "https://calderdale.gov.uk",
"E08000034": "https://kirklees.gov.uk",
"E08000035": "https://leeds.gov.uk",
"E08000036": "https://wakefield.gov.uk",
"E08000037": "https://gateshead.gov.uk",
"E09000001": "https://cityoflondon.gov.uk",
"E09000002": "https://lbbd.gov.uk",
"E09000003": "https://barnet.gov.uk",
"E09000004": "https://bexley.gov.uk",
"E09000005": "https://brent.gov.uk",
"E09000006": "https://bromley.gov.uk",
"E09000007": "https://camden.gov.uk",
"E09000008": "https://croydon.gov.uk",
"E09000009": "https://ealing.gov.uk",
"E09000010": "https://new.enfield.gov.uk",
"E09000011": "https://royalgreenwich.gov.uk",
"E09000012": "https://hackney.gov.uk",
"E09000013": "https://lbhf.gov.uk",
"E09000014": "https://haringey.gov.uk",
"E09000015": "https://harrow.gov.uk",
"E09000016": "https://havering.gov.uk",
"E09000017": "https://hillingdon.gov.uk",
"E09000018": "https://hounslow.gov.uk",
"E09000019": "https://islington.gov.uk",
"E09000020": "https://rbkc.gov.uk",
"E09000021": "https://kingston.gov.uk",
"E09000022": "https://lambeth.gov.uk",
"E09000023": "https://lewisham.gov.uk",
"E09000024": "https://merton.gov.uk",
"E09000025": "https://newham.gov.uk",
"E09000026": "https://redbridge.gov.uk",
"E09000027": "https://richmond.gov.uk",
"E09000028": "https://southwark.gov.uk",
"E09000029": "https://sutton.gov.uk",
"E09000030": "https://towerhamlets.gov.uk",
"E09000031": "https://walthamforest.gov.uk",
"E09000032": "https://wandsworth.gov.uk",
"E09000033": "https://westminster.gov.uk",
"N09000001": "https://antrimandnewtownabbey.gov.uk",
"N09000002": "https://armaghbanbridgecraigavon.gov.uk",
"N09000003": "https://belfastcity.gov.uk",
"N09000004": "https://causewaycoastandglens.gov.uk",
"N09000005": "https://derrystrabane.com",
"N09000006": "https://fermanaghomagh.com",
"N09000007": "https://lisburncastlereagh.gov.uk",
"N09000008": "https://midandeastantrim.gov.uk",
"N09000009": "https://midulstercouncil.org",
"N09000010": "https://newrymournedown.org",
"N09000011": "https://ardsandnorthdown.gov.uk",
"S12000005": "https://clacks.gov.uk",
"S12000006": "https://dumfriesandgalloway.gov.uk",
"S12000008": "https://east-ayrshire.gov.uk",
"S12000010": "https://eastlothian.gov.uk",
"S12000011": "https://eastrenfrewshire.gov.uk",
"S12000013": "https://cne-siar.gov.uk",
"S12000014": "https://falkirk.gov.uk",
"S12000017": "https://highland.gov.uk",
"S12000018": "https://inverclyde.gov.uk",
"S12000019": "https://midlothian.gov.uk",
"S12000020": "https://moray.gov.uk",
"S12000021": "https://north-ayrshire.gov.uk",
"S12000023": "https://orkney.gov.uk",
"S12000026": "https://scotborders.gov.uk",
"S12000027": "https://shetland.gov.uk",
"S12000028": "https://south-ayrshire.gov.uk",
"S12000029": "https://southlanarkshire.gov.uk",
"S12000030": "https://stirling.gov.uk",
"S12000033": "https://aberdeencity.gov.uk",
"S12000034": "https://aberdeenshire.gov.uk",
"S12000035": "https://argyll-bute.gov.uk",
"S12000036": "https://edinburgh.gov.uk",
"S12000038": "https://renfrewshire.gov.uk",
"S12000039": "https://west-dunbarton.gov.uk",
"S12000040": "https://westlothian.gov.uk",
"S12000041": "https://angus.gov.uk",
"S12000042": "https://dundeecity.gov.uk",
"S12000045": "https://eastdunbarton.gov.uk",
"W06000001": "https://anglesey.gov.uk",
"W06000002": "https://gwynedd.llyw.cymru",
"W06000003": "https://conwy.gov.uk",
"W06000004": "https://denbighshire.gov.uk",
"W06000005": "https://flintshire.gov.uk",
"W06000006": "https://wrexham.gov.uk",
"W06000008": "https://ceredigion.gov.uk",
"W06000009": "https://pembrokeshire.gov.uk",
"W06000010": "https://carmarthenshire.gov.wales",
"W06000011": "https://swansea.gov.uk",
"W06000012": "https://npt.gov.uk",
"W06000013": "https://bridgend.gov.uk",
"W06000014": "https://valeofglamorgan.gov.uk",
"W06000015": "https://cardiff.gov.uk",
"W06000016": "https://rctcbc.gov.uk",
"W06000018": "https://caerphilly.gov.uk",
"W06000019": "https://blaenau-gwent.gov.uk",
"W06000020": "https://torfaen.gov.uk",
"W06000021": "https://monmouthshire.gov.uk",
"W06000022": "https://newport.gov.uk",
"W06000023": "https://powys.gov.uk",
"W06000024": "https://merthyr.gov.uk"
}


def _gov_link(postcode):
    return "https://www.gov.uk/find-local-council?postcode=" + quote(postcode)


def council_link(code, postcode):
    return COUNCIL_SITES.get(code) or _gov_link(postcode)


def lookup(postcode):
    if WIB_ENABLED:
        name, addresses = _cached("wib-addr:" + postcode, lambda: _wib_addresses(postcode))
        if addresses:
            return {"postcode": postcode, "council": name, "status": "addresses",
                    "addresses": addresses, "council_page": _gov_link(postcode)}, 200
    council = resolve_council(postcode)
    if not council or not council.get("code"):
        return {"error": "We could not find that postcode."}, 404
    conn = CONNECTORS.get(council["code"])
    if conn:
        addresses = _cached("addr:" + postcode, lambda: conn["addresses"](postcode))
        if addresses:
            return {"postcode": postcode, "council": council["name"], "status": "addresses",
                    "addresses": addresses, "council_page": conn["page"]}, 200
    return {"postcode": postcode, "council": council["name"], "status": "link",
            "council_page": (conn or {}).get("page") or council_link(council["code"], postcode),
            "message": f"Check {council['name']} council's own bin checker for your collection days."}, 200


def register_bins_lookup(app):
    @app.route("/api/bins/lookup")
    def api_bins_lookup():
        pc = (request.args.get("postcode") or "").strip()
        if not pc:
            return jsonify({"error": "Enter a postcode."}), 400
        body, code = lookup(pc)
        return jsonify(body), code

    @app.route("/api/bins/dates")
    def api_bins_dates():
        body, code = dates_for((request.args.get("postcode") or "").strip(),
                               (request.args.get("uprn") or "").strip())
        return jsonify(body), code


def dates_for(pc, uprn):
    """Collection dates for one address. Returns (body, http_code). Used by the route and by reminders."""
    if WIB_ENABLED and uprn.startswith("p:") and pc:
        out = _cached("wib-dates:" + uprn, lambda: _wib_dates(pc, uprn))
        if out:
            out["council_page"] = out.get("council_page") or _gov_link(pc)
            return out, 200
        return {"error": "Collection days are not available for this address right now.",
                "council_page": _gov_link(pc)}, 501
    if not pc or not uprn.isdigit():
        return {"error": "Choose your address first."}, 400
    council = resolve_council(pc) or {}
    conn = CONNECTORS.get(council.get("code"))
    if not conn or not conn.get("dates"):
        return {"error": "Dates for this council are not connected yet.",
                "council_page": (conn or {}).get("page") or council_link(council.get("code"), pc)}, 501
    return conn["dates"](uprn), 200
