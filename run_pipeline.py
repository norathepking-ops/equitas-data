# -*- coding: utf-8 -*-
"""
EQUITAS Data Pipeline v2.0.0
Run: python run_pipeline.py
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ============================================================
# กรอก 2 ค่านี้ก่อนรัน
# ============================================================
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', 'ghp_xxx')  # set via env var or replace here
GITHUB_REPO  = 'norathepking-ops/equitas-data'
BRANCH       = 'main'

# ============================================================
# ค่าเริ่มต้น (ไม่ต้องแก้)
# ============================================================
DATA_FILE       = 'data.json'
PRICE_HIST_DAYS = 365
NEWS_PER_TICKER = 8
MAX_WORKERS     = 4

EDGAR_UA        = 'EQUITAS-Pipeline norathepking@gmail.com'
EDGAR_CIK_URL   = 'https://www.sec.gov/files/company_tickers.json'
EDGAR_FACTS_URL = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
EDGAR_SLEEP     = 0.13

MANUAL_CIKS = {
    'BRK-B': '0001067983',
    'BRK.B': '0001067983',
}

import os, json, time, math, random, re, warnings
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf
import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
warnings.filterwarnings('ignore')

os.environ['GITHUB_TOKEN'] = GITHUB_TOKEN
BKK = timezone(timedelta(hours=7))

# ============================================================
# TICKERS
# ============================================================
TICKERS = [
    {'tkr':'AAPL','name':'Apple Inc.','sector':'Technology','industry':'Consumer Electronics','exch':'NASDAQ'},
    {'tkr':'MSFT','name':'Microsoft Corporation','sector':'Technology','industry':'Software—Infrastructure','exch':'NASDAQ'},
    {'tkr':'NVDA','name':'NVIDIA Corporation','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'GOOGL','name':'Alphabet Inc. Class A','sector':'Communication Services','industry':'Internet Content','exch':'NASDAQ'},
    {'tkr':'AMZN','name':'Amazon.com, Inc.','sector':'Communication Services','industry':'Internet Retail','exch':'NASDAQ'},
    {'tkr':'META','name':'Meta Platforms, Inc.','sector':'Communication Services','industry':'Internet Content','exch':'NASDAQ'},
    {'tkr':'TSLA','name':'Tesla, Inc.','sector':'Consumer Cyclical','industry':'Auto Manufacturers','exch':'NASDAQ'},
    {'tkr':'AVGO','name':'Broadcom Inc.','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'ORCL','name':'Oracle Corporation','sector':'Technology','industry':'Software—Infrastructure','exch':'NYSE'},
    {'tkr':'NFLX','name':'Netflix, Inc.','sector':'Communication Services','industry':'Entertainment','exch':'NASDAQ'},
    {'tkr':'BRK-B','name':'Berkshire Hathaway Inc.','sector':'Financial Services','industry':'Insurance—Diversified','exch':'NYSE'},
    {'tkr':'JPM','name':'JPMorgan Chase & Co.','sector':'Financial Services','industry':'Banks—Diversified','exch':'NYSE'},
    {'tkr':'V','name':'Visa Inc.','sector':'Financial Services','industry':'Credit Services','exch':'NYSE'},
    {'tkr':'MA','name':'Mastercard Incorporated','sector':'Financial Services','industry':'Credit Services','exch':'NYSE'},
    {'tkr':'BAC','name':'Bank of America Corp.','sector':'Financial Services','industry':'Banks—Diversified','exch':'NYSE'},
    {'tkr':'WFC','name':'Wells Fargo & Company','sector':'Financial Services','industry':'Banks—Diversified','exch':'NYSE'},
    {'tkr':'GS','name':'The Goldman Sachs Group','sector':'Financial Services','industry':'Capital Markets','exch':'NYSE'},
    {'tkr':'MS','name':'Morgan Stanley','sector':'Financial Services','industry':'Capital Markets','exch':'NYSE'},
    {'tkr':'AXP','name':'American Express Company','sector':'Financial Services','industry':'Credit Services','exch':'NYSE'},
    {'tkr':'BLK','name':'BlackRock, Inc.','sector':'Financial Services','industry':'Asset Management','exch':'NYSE'},
    {'tkr':'C','name':'Citigroup Inc.','sector':'Financial Services','industry':'Banks—Diversified','exch':'NYSE'},
    {'tkr':'SCHW','name':'Charles Schwab Corp.','sector':'Financial Services','industry':'Capital Markets','exch':'NYSE'},
    {'tkr':'SPGI','name':'S&P Global Inc.','sector':'Financial Services','industry':'Financial Data','exch':'NYSE'},
    {'tkr':'CME','name':'CME Group Inc.','sector':'Financial Services','industry':'Financial Data','exch':'NASDAQ'},
    {'tkr':'COF','name':'Capital One Financial Corp.','sector':'Financial Services','industry':'Credit Services','exch':'NYSE'},
    {'tkr':'LLY','name':'Eli Lilly and Company','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NYSE'},
    {'tkr':'UNH','name':'UnitedHealth Group','sector':'Healthcare','industry':'Healthcare Plans','exch':'NYSE'},
    {'tkr':'JNJ','name':'Johnson & Johnson','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NYSE'},
    {'tkr':'ABBV','name':'AbbVie Inc.','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NYSE'},
    {'tkr':'MRK','name':'Merck & Co., Inc.','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NYSE'},
    {'tkr':'PFE','name':'Pfizer Inc.','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NYSE'},
    {'tkr':'TMO','name':'Thermo Fisher Scientific','sector':'Healthcare','industry':'Diagnostics & Research','exch':'NYSE'},
    {'tkr':'ABT','name':'Abbott Laboratories','sector':'Healthcare','industry':'Medical Devices','exch':'NYSE'},
    {'tkr':'AMGN','name':'Amgen Inc.','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NYSE'},
    {'tkr':'ISRG','name':'Intuitive Surgical','sector':'Healthcare','industry':'Medical Devices','exch':'NASDAQ'},
    {'tkr':'GILD','name':'Gilead Sciences, Inc.','sector':'Healthcare','industry':'Drug Manufacturers','exch':'NASDAQ'},
    {'tkr':'SYK','name':'Stryker Corporation','sector':'Healthcare','industry':'Medical Devices','exch':'NYSE'},
    {'tkr':'VRTX','name':'Vertex Pharmaceuticals','sector':'Healthcare','industry':'Biotechnology','exch':'NASDAQ'},
    {'tkr':'REGN','name':'Regeneron Pharmaceuticals','sector':'Healthcare','industry':'Biotechnology','exch':'NASDAQ'},
    {'tkr':'CVS','name':'CVS Health Corporation','sector':'Healthcare','industry':'Healthcare Plans','exch':'NYSE'},
    {'tkr':'WMT','name':'Walmart Inc.','sector':'Consumer Defensive','industry':'Discount Stores','exch':'NYSE'},
    {'tkr':'COST','name':'Costco Wholesale','sector':'Consumer Defensive','industry':'Discount Stores','exch':'NASDAQ'},
    {'tkr':'PG','name':'Procter & Gamble','sector':'Consumer Defensive','industry':'Household Products','exch':'NYSE'},
    {'tkr':'KO','name':'The Coca-Cola Company','sector':'Consumer Defensive','industry':'Beverages—Non-Alcoholic','exch':'NYSE'},
    {'tkr':'PEP','name':'PepsiCo, Inc.','sector':'Consumer Defensive','industry':'Beverages—Non-Alcoholic','exch':'NASDAQ'},
    {'tkr':'PM','name':'Philip Morris International','sector':'Consumer Defensive','industry':'Tobacco','exch':'NYSE'},
    {'tkr':'MDLZ','name':'Mondelez International','sector':'Consumer Defensive','industry':'Confectioners','exch':'NASDAQ'},
    {'tkr':'HD','name':'The Home Depot, Inc.','sector':'Consumer Cyclical','industry':'Home Improvement Retail','exch':'NYSE'},
    {'tkr':'MCD','name':"McDonald's Corporation",'sector':'Consumer Cyclical','industry':'Restaurants','exch':'NYSE'},
    {'tkr':'NKE','name':'NIKE, Inc.','sector':'Consumer Cyclical','industry':'Footwear & Accessories','exch':'NYSE'},
    {'tkr':'SBUX','name':'Starbucks Corporation','sector':'Consumer Cyclical','industry':'Restaurants','exch':'NASDAQ'},
    {'tkr':'LOW','name':"Lowe's Companies, Inc.",'sector':'Consumer Cyclical','industry':'Home Improvement Retail','exch':'NYSE'},
    {'tkr':'TJX','name':'The TJX Companies, Inc.','sector':'Consumer Cyclical','industry':'Apparel Retail','exch':'NYSE'},
    {'tkr':'BKNG','name':'Booking Holdings Inc.','sector':'Consumer Cyclical','industry':'Travel Services','exch':'NASDAQ'},
    {'tkr':'UBER','name':'Uber Technologies, Inc.','sector':'Consumer Cyclical','industry':'Travel Services','exch':'NYSE'},
    {'tkr':'CRM','name':'Salesforce, Inc.','sector':'Technology','industry':'Software—Application','exch':'NYSE'},
    {'tkr':'AMD','name':'Advanced Micro Devices','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'ADBE','name':'Adobe Inc.','sector':'Technology','industry':'Software—Application','exch':'NASDAQ'},
    {'tkr':'CSCO','name':'Cisco Systems, Inc.','sector':'Technology','industry':'Communication Equipment','exch':'NASDAQ'},
    {'tkr':'IBM','name':'International Business Machines','sector':'Technology','industry':'IT Services','exch':'NYSE'},
    {'tkr':'TXN','name':'Texas Instruments','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'INTC','name':'Intel Corporation','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'QCOM','name':'QUALCOMM Incorporated','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'NOW','name':'ServiceNow, Inc.','sector':'Technology','industry':'Software—Application','exch':'NYSE'},
    {'tkr':'INTU','name':'Intuit Inc.','sector':'Technology','industry':'Software—Application','exch':'NASDAQ'},
    {'tkr':'AMAT','name':'Applied Materials, Inc.','sector':'Technology','industry':'Semiconductor Equipment','exch':'NASDAQ'},
    {'tkr':'MU','name':'Micron Technology, Inc.','sector':'Technology','industry':'Semiconductors','exch':'NASDAQ'},
    {'tkr':'KLAC','name':'KLA Corporation','sector':'Technology','industry':'Semiconductor Equipment','exch':'NASDAQ'},
    {'tkr':'PLTR','name':'Palantir Technologies','sector':'Technology','industry':'Software—Infrastructure','exch':'NASDAQ'},
    {'tkr':'CRWD','name':'CrowdStrike Holdings, Inc.','sector':'Technology','industry':'Software—Infrastructure','exch':'NASDAQ'},
    {'tkr':'PANW','name':'Palo Alto Networks','sector':'Technology','industry':'Software—Infrastructure','exch':'NASDAQ'},
    {'tkr':'ACN','name':'Accenture plc','sector':'Technology','industry':'IT Services','exch':'NYSE'},
    {'tkr':'CAT','name':'Caterpillar Inc.','sector':'Industrials','industry':'Farm & Heavy Construction','exch':'NYSE'},
    {'tkr':'GE','name':'GE Aerospace','sector':'Industrials','industry':'Aerospace & Defense','exch':'NYSE'},
    {'tkr':'HON','name':'Honeywell International','sector':'Industrials','industry':'Specialty Industrial','exch':'NASDAQ'},
    {'tkr':'BA','name':'The Boeing Company','sector':'Industrials','industry':'Aerospace & Defense','exch':'NYSE'},
    {'tkr':'DE','name':'Deere & Company','sector':'Industrials','industry':'Farm & Heavy Construction','exch':'NYSE'},
    {'tkr':'LMT','name':'Lockheed Martin Corp.','sector':'Industrials','industry':'Aerospace & Defense','exch':'NYSE'},
    {'tkr':'RTX','name':'RTX Corporation','sector':'Industrials','industry':'Aerospace & Defense','exch':'NYSE'},
    {'tkr':'UPS','name':'United Parcel Service, Inc.','sector':'Industrials','industry':'Air Freight & Logistics','exch':'NYSE'},
    {'tkr':'ETN','name':'Eaton Corporation plc','sector':'Industrials','industry':'Specialty Industrial','exch':'NYSE'},
    {'tkr':'XOM','name':'Exxon Mobil Corporation','sector':'Energy','industry':'Oil & Gas Integrated','exch':'NYSE'},
    {'tkr':'CVX','name':'Chevron Corporation','sector':'Energy','industry':'Oil & Gas Integrated','exch':'NYSE'},
    {'tkr':'COP','name':'ConocoPhillips','sector':'Energy','industry':'Oil & Gas E&P','exch':'NYSE'},
    {'tkr':'OXY','name':'Occidental Petroleum Corp.','sector':'Energy','industry':'Oil & Gas E&P','exch':'NYSE'},
    {'tkr':'EOG','name':'EOG Resources, Inc.','sector':'Energy','industry':'Oil & Gas E&P','exch':'NYSE'},
    {'tkr':'VZ','name':'Verizon Communications','sector':'Communication Services','industry':'Telecom Services','exch':'NYSE'},
    {'tkr':'T','name':'AT&T Inc.','sector':'Communication Services','industry':'Telecom Services','exch':'NYSE'},
    {'tkr':'TMUS','name':'T-Mobile US, Inc.','sector':'Communication Services','industry':'Telecom Services','exch':'NASDAQ'},
    {'tkr':'DIS','name':'The Walt Disney Company','sector':'Communication Services','industry':'Entertainment','exch':'NYSE'},
    {'tkr':'CMCSA','name':'Comcast Corporation','sector':'Communication Services','industry':'Telecom Services','exch':'NASDAQ'},
    {'tkr':'NEE','name':'NextEra Energy, Inc.','sector':'Utilities','industry':'Utilities—Regulated Electric','exch':'NYSE'},
    {'tkr':'DUK','name':'Duke Energy Corporation','sector':'Utilities','industry':'Utilities—Regulated Electric','exch':'NYSE'},
    {'tkr':'SO','name':'The Southern Company','sector':'Utilities','industry':'Utilities—Regulated Electric','exch':'NYSE'},
    {'tkr':'AMT','name':'American Tower Corporation','sector':'Real Estate','industry':'REIT—Specialty','exch':'NYSE'},
    {'tkr':'PLD','name':'Prologis, Inc.','sector':'Real Estate','industry':'REIT—Industrial','exch':'NYSE'},
    {'tkr':'EQIX','name':'Equinix, Inc.','sector':'Real Estate','industry':'REIT—Specialty','exch':'NASDAQ'},
    {'tkr':'LIN','name':'Linde plc','sector':'Basic Materials','industry':'Specialty Chemicals','exch':'NASDAQ'},
    {'tkr':'SHW','name':'The Sherwin-Williams Company','sector':'Basic Materials','industry':'Specialty Chemicals','exch':'NYSE'},
    {'tkr':'FCX','name':'Freeport-McMoRan Inc.','sector':'Basic Materials','industry':'Copper','exch':'NYSE'},
]

print(f'Total tickers: {len(TICKERS)}')

# ============================================================
# HELPERS
# ============================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
]

def make_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

def make_edgar_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent': EDGAR_UA,
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate',
    })
    retry = Retry(total=4, backoff_factor=2.0, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s

EDGAR_SESSION = make_edgar_session()

def safe(v, default=None):
    if v is None: return default
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f): return default
        return f
    except (TypeError, ValueError):
        return default

def safe_round(v, digits=2, default=None):
    f = safe(v, None)
    return round(f, digits) if f is not None else default

def to_billions(v):
    f = safe(v)
    return round(f / 1e9, 2) if f is not None else None

def to_millions(v):
    f = safe(v)
    return round(f / 1e6, 1) if f is not None else None

def pct(v):
    f = safe(v)
    return round(f * 100, 2) if f is not None else None

print('✓ Helpers loaded')

# ============================================================
# EDGAR CIK MAP
# ============================================================
def load_edgar_cik_map():
    resp = EDGAR_SESSION.get(EDGAR_CIK_URL, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    cik_map = {}
    for entry in raw.values():
        tkr = entry['ticker'].upper()
        cik = str(entry['cik_str']).zfill(10)
        cik_map[tkr] = cik
    cik_map.update(MANUAL_CIKS)
    return cik_map

print('Loading CIK map from EDGAR...')
CIK_MAP = load_edgar_cik_map()
print(f'✓ CIK map loaded: {len(CIK_MAP)} tickers')
for chk in ['AAPL', 'MSFT', 'NVDA', 'BRK-B', 'IREN']:
    status = CIK_MAP.get(chk, 'NOT FOUND (yfinance fallback)')
    print(f'  {chk:6s}: {status}')

# ============================================================
# EDGAR XBRL FETCHER
# ============================================================
XBRL_CONCEPTS = {
    'revenue': [
        'Revenues',
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'SalesRevenueNet',
        'RevenuesNetOfInterestExpense',
        'InterestAndDividendIncomeOperating',
    ],
    'gross_profit':   ['GrossProfit'],
    'op_income':      ['OperatingIncomeLoss'],
    'net_income':     ['NetIncomeLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic'],
    'eps_basic':      ['EarningsPerShareBasic'],
    'eps_diluted':    ['EarningsPerShareDiluted'],
    'total_assets':   ['Assets'],
    'total_liab':     ['Liabilities'],
    'total_equity':   ['StockholdersEquity',
                       'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
    'cash':           ['CashAndCashEquivalentsAtCarryingValue',
                       'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents'],
    'total_debt':     ['LongTermDebt', 'LongTermDebtAndCapitalLeaseObligations'],
    'current_assets': ['AssetsCurrent'],
    'current_liab':   ['LiabilitiesCurrent'],
    'op_cf':          ['NetCashProvidedByUsedInOperatingActivities'],
    'capex':          ['PaymentsToAcquirePropertyPlantAndEquipment',
                       'PaymentsForCapitalImprovements'],
    'investing_cf':   ['NetCashProvidedByUsedInInvestingActivities'],
    'financing_cf':   ['NetCashProvidedByUsedInFinancingActivities'],
}

def get_annual_values(usgaap, concept_list, n_years=5):
    best_result = []
    best_max_year = 0
    for concept in concept_list:
        if concept not in usgaap:
            continue
        unit_key = 'USD/shares' if concept.startswith('EarningsPerShare') else 'USD'
        entries = usgaap[concept].get('units', {}).get(unit_key, [])
        if not entries:
            continue
        annual = [e for e in entries
                  if e.get('form') in ('10-K', '10-K405', '20-F', '40-F')
                  and e.get('accn')]
        if not annual:
            continue
        by_year = {}
        for e in annual:
            year = int(e['end'][:4])
            filed = e.get('filed', '0000-00-00')
            if year not in by_year or filed > by_year[year]['filed']:
                by_year[year] = e
        sorted_years = sorted(by_year.items(), key=lambda x: x[0], reverse=True)[:n_years]
        result = [(yr, e['val']) for yr, e in sorted_years]
        # Keep the concept whose most recent year is latest (handles ASC606 transition, e.g. AAPL)
        if result and result[0][0] > best_max_year:
            best_max_year = result[0][0]
            best_result = result
    return best_result

def fetch_edgar_financials(tkr, cik):
    time.sleep(EDGAR_SLEEP)
    url = EDGAR_FACTS_URL.format(cik=cik)
    try:
        resp = EDGAR_SESSION.get(url, timeout=45)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        usgaap = resp.json().get('facts', {}).get('us-gaap', {})
    except Exception:
        return None
    if not usgaap:
        return None

    def get(key, n=5):
        return get_annual_values(usgaap, XBRL_CONCEPTS[key], n)

    rev_d = dict(get('revenue'))
    if not rev_d:
        return None

    gp_d    = dict(get('gross_profit'))
    oi_d    = dict(get('op_income'))
    ni_d    = dict(get('net_income'))
    epsb_d  = dict(get('eps_basic'))
    epsd_d  = dict(get('eps_diluted'))
    ta_d    = dict(get('total_assets'))
    tl_d    = dict(get('total_liab'))
    eq_d    = dict(get('total_equity'))
    cash_d  = dict(get('cash'))
    debt_d  = dict(get('total_debt'))
    ca_d    = dict(get('current_assets'))
    cl_d    = dict(get('current_liab'))
    ocf_d   = dict(get('op_cf'))
    capex_d = dict(get('capex'))
    icf_d   = dict(get('investing_cf'))
    fcfd_d  = dict(get('financing_cf'))

    income_stmt = []
    for yr in sorted(rev_d, reverse=True)[:5]:
        income_stmt.append({
            'year':             yr,
            'revenue':          to_millions(rev_d[yr]),
            'gross_profit':     to_millions(gp_d.get(yr)),
            'operating_income': to_millions(oi_d.get(yr)),
            'net_income':       to_millions(ni_d.get(yr)),
            'eps_basic':        safe_round(epsb_d.get(yr), 2),
            'eps_diluted':      safe_round(epsd_d.get(yr), 2),
        })

    balance_sheet = []
    for yr in sorted(set(ta_d) | set(eq_d), reverse=True)[:5]:
        balance_sheet.append({
            'year':                yr,
            'total_assets':        to_millions(ta_d.get(yr)),
            'total_liabilities':   to_millions(tl_d.get(yr)),
            'total_equity':        to_millions(eq_d.get(yr)),
            'cash':                to_millions(cash_d.get(yr)),
            'total_debt':          to_millions(debt_d.get(yr)),
            'current_assets':      to_millions(ca_d.get(yr)),
            'current_liabilities': to_millions(cl_d.get(yr)),
        })

    cash_flow = []
    for yr in sorted(set(ocf_d) | set(capex_d), reverse=True)[:5]:
        ocf_v   = ocf_d.get(yr)
        capex_v = capex_d.get(yr)
        fcf_v   = (ocf_v - capex_v) if (ocf_v is not None and capex_v is not None) else None
        cash_flow.append({
            'year':         yr,
            'operating_cf': to_millions(ocf_v),
            'capex':        to_millions(-capex_v) if capex_v is not None else None,
            'fcf':          to_millions(fcf_v),
            'investing_cf': to_millions(icf_d.get(yr)),
            'financing_cf': to_millions(fcfd_d.get(yr)),
        })

    return {
        'income_stmt':   income_stmt,
        'balance_sheet': balance_sheet,
        'cash_flow':     cash_flow,
    }

print('✓ EDGAR XBRL fetcher ready')

# ============================================================
# NEWS
# ============================================================
BULL_KW = ['beat','beats','surge','soar','rally','upgrade','raises','raised','record',
           'strong','outperform','buy rating','price target raised','tops','exceeds',
           'expands','partnership','wins','breakthrough','approval','approved','launches',
           'milestone','jumps','gains','higher','optimistic','bullish','profit',
           'dividend increase','buyback','repurchase','guidance raised']
BEAR_KW = ['miss','misses','plunge','drop','falls','tumble','downgrade','cuts','lowered',
           'weak','underperform','sell rating','price target cut','lawsuit','investigation',
           'probe','recall','warning','concern','concerns','risk','bearish','loss',
           'losses','layoff','layoffs','fired','guidance cut','profit warning','fraud',
           'short report','antitrust','fined','penalty']

def classify_news(title, summary=''):
    text = (title + ' ' + summary).lower()
    bull = sum(1 for kw in BULL_KW if kw in text)
    bear = sum(1 for kw in BEAR_KW if kw in text)
    if bull > bear: return 'bull'
    if bear > bull: return 'bear'
    return 'neut'

def fetch_news(tkr, max_items=NEWS_PER_TICKER, timeout=12):
    items = []
    # Yahoo Finance RSS — 2 attempts
    for attempt in range(2):
        try:
            url  = f'https://feeds.finance.yahoo.com/rss/2.0/headline?s={tkr}&region=US&lang=en-US'
            resp = SESSION.get(url, timeout=timeout)
            if resp.status_code == 200 and len(resp.text) > 200:
                feed = feedparser.parse(resp.text)
                for entry in feed.entries[:max_items]:
                    title   = entry.get('title', '')
                    summary = re.sub(r'<[^>]+>', '', entry.get('summary', ''))[:240]
                    pub     = entry.get('published_parsed') or entry.get('updated_parsed')
                    ts      = int(time.mktime(pub)) if pub else int(time.time())
                    items.append({'title': title[:200], 'summary': summary.strip(),
                                   'source': 'Yahoo Finance', 'url': entry.get('link', ''),
                                   'ts': ts, 'tag': classify_news(title, summary)})
                if items:
                    break
        except Exception:
            pass
        if attempt == 0 and not items:
            time.sleep(1)
    # Google News RSS fallback — 2 attempts
    if not items:
        for attempt in range(2):
            try:
                url  = f'https://news.google.com/rss/search?q={tkr}+stock&hl=en-US&gl=US&ceid=US:en'
                resp = SESSION.get(url, timeout=timeout)
                if resp.status_code == 200:
                    feed = feedparser.parse(resp.text)
                    for entry in feed.entries[:max_items]:
                        title = entry.get('title', '')
                        src   = getattr(entry, 'source', {}).get('title', 'Google News') if hasattr(entry, 'source') else 'Google News'
                        pub   = entry.get('published_parsed')
                        ts    = int(time.mktime(pub)) if pub else int(time.time())
                        items.append({'title': title[:200], 'summary': '', 'source': src,
                                       'url': entry.get('link', ''), 'ts': ts,
                                       'tag': classify_news(title, '')})
                    if items:
                        break
            except Exception:
                pass
            if attempt == 0 and not items:
                time.sleep(1)
    return items

# ============================================================
# FETCH TICKER
# ============================================================
def fetch_ticker(meta, edgar_data=None, prev_record=None):
    tkr     = meta['tkr']
    now_ts  = datetime.now(timezone.utc)
    cache   = (prev_record or {}).get('_cache', {})

    def is_fresh(key, ttl_days):
        ts_str = cache.get(key)
        if not ts_str:
            return False
        try:
            ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return (now_ts - ts).total_seconds() < ttl_days * 86400
        except Exception:
            return False

    try:
        t    = yf.Ticker(tkr)
        info = t.info or {}

        if not info.get('regularMarketPrice') and not info.get('currentPrice') and not info.get('previousClose'):
            return {'tkr': tkr, 'meta': meta, 'error': 'no_data'}

        price      = safe(info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose'))
        prev_close = safe(info.get('previousClose'))
        change     = round(price - prev_close, 2) if (price and prev_close) else 0
        change_pct = round((change / prev_close) * 100, 2) if (prev_close and prev_close > 0) else 0
        mcap       = to_billions(info.get('marketCap'))

        # Pre-market / after-hours (free via yfinance info)
        pre_price  = safe_round(info.get('preMarketPrice'), 2)
        pre_chg    = safe_round(info.get('preMarketChange'), 2)
        pre_pct_raw = safe(info.get('preMarketChangePercent'))
        pre_pct    = round(pre_pct_raw * 100, 2) if pre_pct_raw is not None else None

        post_price = safe_round(info.get('postMarketPrice'), 2)
        post_chg   = safe_round(info.get('postMarketChange'), 2)
        post_pct_raw = safe(info.get('postMarketChangePercent'))
        post_pct   = round(post_pct_raw * 100, 2) if post_pct_raw is not None else None

        pe        = safe_round(info.get('trailingPE'), 1)
        ev_ebitda = safe_round(info.get('enterpriseToEbitda'), 1)
        div_y     = pct(info.get('dividendYield'))
        if div_y is not None and div_y > 50:
            div_y = round(div_y / 100, 2) if div_y > 100 else round(div_y, 2)
        beta      = safe_round(info.get('beta'), 2)
        rev       = to_billions(info.get('totalRevenue'))
        op_m      = pct(info.get('operatingMargins'))
        net_m     = pct(info.get('profitMargins'))
        growth    = pct(info.get('revenueGrowth'))
        fcf       = safe(info.get('freeCashflow'))
        total_rev = safe(info.get('totalRevenue'))
        fcf_m     = round((fcf / total_rev) * 100, 2) if (fcf and total_rev and total_rev > 0) else None
        wk52_high = safe_round(info.get('fiftyTwoWeekHigh'), 2)
        wk52_low  = safe_round(info.get('fiftyTwoWeekLow'), 2)
        avg_vol   = safe(info.get('averageVolume'))
        avg_vol_m = round(avg_vol / 1e6, 1) if avg_vol else None
        currency  = info.get('currency', 'USD')
        biz_desc  = (info.get('longBusinessSummary') or '')[:600]
        employees = info.get('fullTimeEmployees')
        country   = info.get('country', '')
        website   = info.get('website', '')

        price_hist = []
        try:
            hist = t.history(period='1y', interval='1d', auto_adjust=True)
            if not hist.empty:
                for idx, row in hist.iterrows():
                    price_hist.append({
                        'd': idx.strftime('%Y-%m-%d'),
                        'c': round(float(row['Close']), 2),
                        'v': int(row['Volume']) if not math.isnan(row['Volume']) else 0,
                    })
        except Exception:
            pass

        income_stmt = []
        balance_sheet = []
        cash_flow = []

        if edgar_data:
            income_stmt   = edgar_data['income_stmt']
            balance_sheet = edgar_data['balance_sheet']
            cash_flow     = edgar_data['cash_flow']
        else:
            try:
                is_df = t.income_stmt
                if is_df is not None and not is_df.empty:
                    for col in is_df.columns[:5]:
                        yr = col.year
                        income_stmt.append({
                            'year':             yr,
                            'revenue':          to_millions(is_df.loc['Total Revenue', col]) if 'Total Revenue' in is_df.index else None,
                            'gross_profit':     to_millions(is_df.loc['Gross Profit', col]) if 'Gross Profit' in is_df.index else None,
                            'operating_income': to_millions(is_df.loc['Operating Income', col]) if 'Operating Income' in is_df.index else None,
                            'net_income':       to_millions(is_df.loc['Net Income', col]) if 'Net Income' in is_df.index else None,
                            'eps_basic':        safe_round(is_df.loc['Basic EPS', col], 2) if 'Basic EPS' in is_df.index else None,
                            'eps_diluted':      safe_round(is_df.loc['Diluted EPS', col], 2) if 'Diluted EPS' in is_df.index else None,
                        })
            except Exception:
                pass
            try:
                bs_df = t.balance_sheet
                if bs_df is not None and not bs_df.empty:
                    for col in bs_df.columns[:5]:
                        yr = col.year
                        balance_sheet.append({
                            'year':                yr,
                            'total_assets':        to_millions(bs_df.loc['Total Assets', col]) if 'Total Assets' in bs_df.index else None,
                            'total_liabilities':   to_millions(bs_df.loc['Total Liabilities Net Minority Interest', col]) if 'Total Liabilities Net Minority Interest' in bs_df.index else None,
                            'total_equity':        to_millions(bs_df.loc['Stockholders Equity', col]) if 'Stockholders Equity' in bs_df.index else None,
                            'cash':                to_millions(bs_df.loc['Cash And Cash Equivalents', col]) if 'Cash And Cash Equivalents' in bs_df.index else None,
                            'total_debt':          to_millions(bs_df.loc['Total Debt', col]) if 'Total Debt' in bs_df.index else None,
                            'current_assets':      to_millions(bs_df.loc['Current Assets', col]) if 'Current Assets' in bs_df.index else None,
                            'current_liabilities': to_millions(bs_df.loc['Current Liabilities', col]) if 'Current Liabilities' in bs_df.index else None,
                        })
            except Exception:
                pass
            try:
                cf_df = t.cashflow
                if cf_df is not None and not cf_df.empty:
                    for col in cf_df.columns[:5]:
                        yr = col.year
                        cash_flow.append({
                            'year':         yr,
                            'operating_cf': to_millions(cf_df.loc['Operating Cash Flow', col]) if 'Operating Cash Flow' in cf_df.index else None,
                            'investing_cf': to_millions(cf_df.loc['Investing Cash Flow', col]) if 'Investing Cash Flow' in cf_df.index else None,
                            'financing_cf': to_millions(cf_df.loc['Financing Cash Flow', col]) if 'Financing Cash Flow' in cf_df.index else None,
                            'capex':        to_millions(cf_df.loc['Capital Expenditure', col]) if 'Capital Expenditure' in cf_df.index else None,
                            'fcf':          to_millions(cf_df.loc['Free Cash Flow', col]) if 'Free Cash Flow' in cf_df.index else None,
                        })
            except Exception:
                pass

        roe          = pct(info.get('returnOnEquity'))
        roa          = pct(info.get('returnOnAssets'))
        debt_eq      = safe_round(info.get('debtToEquity'), 2)
        if debt_eq and debt_eq > 5: debt_eq = round(debt_eq / 100, 2)
        current_ratio = safe_round(info.get('currentRatio'), 2)
        quick_ratio  = safe_round(info.get('quickRatio'), 2)
        gross_m      = pct(info.get('grossMargins'))
        eps_ttm      = safe_round(info.get('trailingEps'), 2)
        eps_fwd      = safe_round(info.get('forwardEps'), 2)
        pe_fwd       = safe_round(info.get('forwardPE'), 1)
        peg          = safe_round(info.get('pegRatio'), 2)
        ps           = safe_round(info.get('priceToSalesTrailing12Months'), 2)
        pb           = safe_round(info.get('priceToBook'), 2)
        bvps         = safe_round(info.get('bookValue'), 2)

        if edgar_data and income_stmt:
            last_is = income_stmt[0]
            rev_m = last_is.get('revenue')
            if rev_m and rev_m > 0:
                gp_m = last_is.get('gross_profit')
                oi_m = last_is.get('operating_income')
                ni_m = last_is.get('net_income')
                if gp_m is not None: gross_m = round(gp_m / rev_m * 100, 2)
                if oi_m is not None: op_m    = round(oi_m / rev_m * 100, 2)
                if ni_m is not None: net_m   = round(ni_m / rev_m * 100, 2)
                rev = round(rev_m / 1000, 2)
            else:
                rev_m = None
            if cash_flow:
                fcf_v = cash_flow[0].get('fcf')
                if fcf_v is not None and rev_m and rev_m > 0:
                    fcf_m = round(fcf_v / rev_m * 100, 2)

        if edgar_data and income_stmt and balance_sheet:
            ni_v = income_stmt[0].get('net_income')
            eq_v = balance_sheet[0].get('total_equity')
            ta_v = balance_sheet[0].get('total_assets')
            if ni_v is not None and eq_v and eq_v > 0:
                roe = round(ni_v / eq_v * 100, 2)
            if ni_v is not None and ta_v and ta_v > 0:
                roa = round(ni_v / ta_v * 100, 2)

        # recommendations — cached 7 days (saves 1 yfinance call/ticker/day)
        rec_data     = None
        new_analyst_ts = cache.get('analyst_ts')
        if is_fresh('analyst_ts', 7) and prev_record and prev_record.get('analyst', {}).get('recommendations'):
            rec_data = prev_record['analyst']['recommendations']
        else:
            try:
                recs = t.recommendations
                if recs is not None and not recs.empty:
                    latest = recs.iloc[0]
                    rec_data = {
                        'strong_buy':  int(latest.get('strongBuy', 0)),
                        'buy':         int(latest.get('buy', 0)),
                        'hold':        int(latest.get('hold', 0)),
                        'sell':        int(latest.get('sell', 0)),
                        'strong_sell': int(latest.get('strongSell', 0)),
                    }
                    new_analyst_ts = now_ts.isoformat()
            except Exception:
                pass

        pt_high      = safe_round(info.get('targetHighPrice'), 2)
        pt_low       = safe_round(info.get('targetLowPrice'), 2)
        pt_mean      = safe_round(info.get('targetMeanPrice'), 2)
        pt_median    = safe_round(info.get('targetMedianPrice'), 2)
        rec_mean     = safe_round(info.get('recommendationMean'), 2)
        num_analysts = info.get('numberOfAnalystOpinions')

        news = fetch_news(tkr)

        return {
            'tkr': tkr, 'meta': meta,
            'quote': {
                'price': price, 'change': change, 'changePct': change_pct,
                'prevClose': prev_close, 'currency': currency,
                'wk52High': wk52_high, 'wk52Low': wk52_low, 'avgVolM': avg_vol_m,
                'preMarket':  {'price': pre_price,  'change': pre_chg,  'changePct': pre_pct}  if pre_price  is not None else None,
                'postMarket': {'price': post_price, 'change': post_chg, 'changePct': post_pct} if post_price is not None else None,
            },
            'fundamentals': {
                'mcap': mcap, 'pe': pe, 'ev': ev_ebitda, 'divY': div_y,
                'beta': beta, 'rev': rev, 'opM': op_m, 'netM': net_m,
                'growth': growth, 'fcfM': fcf_m,
            },
            'ratios': {
                'grossM': gross_m, 'roe': roe, 'roa': roa, 'debtEq': debt_eq,
                'currentRatio': current_ratio, 'quickRatio': quick_ratio,
                'epsTTM': eps_ttm, 'epsFwd': eps_fwd, 'peFwd': pe_fwd,
                'peg': peg, 'ps': ps, 'pb': pb, 'bvps': bvps,
            },
            'profile': {
                'description': biz_desc, 'employees': employees,
                'country': country, 'website': website,
            },
            'priceHistory': price_hist,
            'incomeStmt':   income_stmt,
            'balanceSheet': balance_sheet,
            'cashFlow':     cash_flow,
            'analyst': {
                'recommendations': rec_data, 'numAnalysts': num_analysts,
                'recMean': rec_mean, 'ptHigh': pt_high, 'ptLow': pt_low,
                'ptMean': pt_mean, 'ptMedian': pt_median,
            },
            'news': news,
            '_cache': {'analyst_ts': new_analyst_ts},
        }

    except Exception as e:
        return {'tkr': tkr, 'meta': meta, 'error': str(e)[:120]}


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    # Diagnostic: test AAPL first
    print('\nDiagnostic: testing AAPL...')
    _cik  = CIK_MAP.get('AAPL')
    _edg  = fetch_edgar_financials('AAPL', _cik) if _cik else None
    _test = fetch_ticker(TICKERS[0], edgar_data=_edg)
    if 'error' in _test:
        print(f'  ✗ Error: {_test["error"]}')
        print('  Yahoo may be rate-limiting this IP. Try again in 10 minutes.')
        raise SystemExit(1)
    _src = 'EDGAR' if _edg else 'yfinance'
    print(f'  ✓ Price ${_test["quote"]["price"]}  |  Income stmt: {len(_test["incomeStmt"])}Y [{_src}]  |  News: {len(_test["news"])}')
    print('  Diagnostic OK\n')

    # PHASE A: EDGAR sequential
    edgar_results = {}
    edgar_ok = edgar_miss = 0
    for meta in tqdm(TICKERS, desc='Phase A: EDGAR financials'):
        tkr = meta['tkr']
        cik = CIK_MAP.get(tkr)
        if cik:
            data = fetch_edgar_financials(tkr, cik)
            if data:
                edgar_results[tkr] = data
                edgar_ok += 1
            else:
                edgar_miss += 1
        else:
            edgar_miss += 1
    print(f'\n✓ Phase A: {edgar_ok} EDGAR, {edgar_miss} yfinance fallback\n')

    # Load previous data.json for stale-if-error cache
    prev_map = {}
    try:
        prev_local = os.path.join(os.path.dirname(__file__), DATA_FILE)
        if os.path.exists(prev_local):
            with open(prev_local, 'r', encoding='utf-8') as f:
                prev_json = json.load(f)
            for r in prev_json.get('tickers', []):
                prev_map[r['tkr']] = r
            print(f'  Cache: loaded {len(prev_map)} previous records from {DATA_FILE}')
    except Exception as ex_c:
        print(f'  Cache: could not load previous {DATA_FILE} ({ex_c})')

    # PHASE B: yfinance parallel
    results = []
    errors  = []
    start   = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_ticker, m, edgar_results.get(m['tkr']), prev_map.get(m['tkr'])): m for m in TICKERS}
        for f in tqdm(as_completed(futures), total=len(futures), desc='Phase B: yfinance quote+analyst'):
            r = f.result()
            (errors if 'error' in r else results).append(r)
    elapsed = time.time() - start
    results.sort(key=lambda r: r['tkr'])
    print(f'\n✓ Done in {elapsed:.1f}s  |  Success: {len(results)}/{len(TICKERS)}  |  Errors: {len(errors)}')
    if errors:
        print('Failed:', [e['tkr'] for e in errors[:10]])
        # Stale-if-error: use previous data for failed tickers (up to 7 days old)
        stale_used = 0
        now_ts = datetime.now(timezone.utc)
        for e in errors:
            tkr = e['tkr']
            if tkr in prev_map:
                prev = prev_map[tkr].copy()
                gen_str = prev_json.get('meta', {}).get('generated_at_utc', '')
                try:
                    gen_ts = datetime.fromisoformat(gen_str.replace('Z', '+00:00'))
                    if (now_ts - gen_ts).total_seconds() > 7 * 86400:
                        continue
                except Exception:
                    pass
                prev['_stale'] = True
                results.append(prev)
                stale_used += 1
        if stale_used:
            print(f'✓ Stale cache used for {stale_used} failed ticker(s)')
        results.sort(key=lambda r: r['tkr'])

    # Build JSON
    now_bkk = datetime.now(BKK)
    now_utc = datetime.now(timezone.utc)
    payload = {
        'meta': {
            'generated_at_utc':  now_utc.isoformat(),
            'generated_at_bkk':  now_bkk.isoformat(),
            'generated_display': now_bkk.strftime('%Y-%m-%d %H:%M GMT+7'),
            'ticker_count':      len(results),
            'failed_count':      len(errors),
            'failed_tickers':    [e['tkr'] for e in errors],
            'pipeline_version':  '2.0.0',
            'data_sources':      ['SEC EDGAR XBRL', 'yfinance', 'Yahoo Finance RSS', 'Google News RSS'],
            'edgar_success':     edgar_ok,
            'edgar_fallback':    edgar_miss,
        },
        'tickers': results,
    }
    json_str = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
    size_mb  = len(json_str.encode('utf-8')) / 1024 / 1024
    out_path = os.path.join(os.path.dirname(__file__), DATA_FILE)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(json_str)
    print(f'✓ Saved {out_path}  ({size_mb:.2f} MB)')

    # Push to GitHub
    if GITHUB_TOKEN.startswith('ghp_xxx'):
        print('\n⚠  GITHUB_TOKEN not set — skipping push.')
        print('   แก้ไข GITHUB_TOKEN ใน run_pipeline.py แล้วรันใหม่')
    else:
        from github import Github, GithubException
        try:
            gh   = Github(GITHUB_TOKEN)
            repo = gh.get_repo(GITHUB_REPO)
            msg  = f'data: update {payload["meta"]["generated_display"]}'
            try:
                existing = repo.get_contents(DATA_FILE, ref=BRANCH)
                repo.update_file(DATA_FILE, msg, json_str, existing.sha, branch=BRANCH)
                print(f'✓ Updated {DATA_FILE} → {GITHUB_REPO}@{BRANCH}')
            except GithubException as e:
                if e.status == 404:
                    repo.create_file(DATA_FILE, msg, json_str, branch=BRANCH)
                    print(f'✓ Created {DATA_FILE} → {GITHUB_REPO}@{BRANCH}')
                else:
                    raise
        except Exception as e:
            print(f'✗ GitHub push failed: {e}')

    print('\nDone. Run again in 4-8 hours for fresh data.')
