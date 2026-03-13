"""
Arbitrage Monitor Bot — Polymarket <-> OddsPapi
Futebol, Ténis, Esports (CS2, Dota2, LoL, Valorant)
Matching rigoroso | Alertas via Telegram
"""

import asyncio
import aiohttp
import logging
import os
import json
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ODDS_API_KEY     = os.getenv("ODDS_API_KEY", "55be989361b732d8ed74b8a3fed378fb")  # fallback desportos trad.
ODDSPAPI_KEY     = os.getenv("ODDSPAPI_KEY", "4fdc5784-b9f3-413c-8e78-bbe43ba3d385")
GAMMA_API_BASE   = "https://gamma-api.polymarket.com"
ODDS_API_BASE    = "https://api.the-odds-api.com/v4"
ODDSPAPI_BASE    = "https://api.oddspapi.io/v4"

MIN_EDGE      = 0.03
POLY_FEE      = 0.02
SCAN_INTERVAL = 10

# ── OddsPapi Sport IDs ──
SPORT_FOOTBALL  = 10
SPORT_TENNIS    = 13
SPORT_BASKETBALL= 18  # nota: 18 é LoL no OddsPapi; basket é diferente — usamos The Odds API para NBA/NHL
ESPORT_IDS = {17: "CS2", 16: "Dota 2", 18: "League of Legends", 61: "Valorant", 56: "Call of Duty"}

# ── OddsPapi Tournament IDs (futebol) ──
FOOTBALL_TOURNAMENT_IDS = [
    17,    # Premier League
    8,     # La Liga
    35,    # Bundesliga
    31,    # Serie A
    34,    # Ligue 1
    132,   # Champions League
    133,   # Europa League
    87,    # MLS
    13,    # Primeira Liga (Portugal)
    16,    # Eredivisie
    23,    # Brazil Série A
    44,    # Argentina Primera División
]

# ── OddsPapi Tournament IDs (ténis) ──
TENNIS_TOURNAMENT_IDS = [
    2736,  # ATP Indian Wells
    2737,  # WTA Indian Wells
    2738,  # ATP Miami
    2739,  # WTA Miami
    # Nota: se não existirem, o bot ignora silenciosamente
]

# ── The Odds API — desportos sem cobertura no OddsPapi ──
TRADITIONAL_SPORT_KEYS = [
    "basketball_nba",
    "icehockey_nhl",
    "americanfootball_nfl",
    "baseball_mlb",
    "basketball_ncaab",
]

# ─── DATA CLASSES ─────────────────────────────────────────────────────────────
@dataclass
class PolyMarket:
    condition_id: str
    question: str
    yes_price: float
    no_price: float
    url: str = ""

@dataclass
class OddsEvent:
    event_id: str
    sport: str
    home_team: str
    away_team: str
    best_odds_home: float
    best_odds_away: float
    best_bookmaker_home: str
    best_bookmaker_away: str
    draw_odds: float = 0.0
    draw_bookmaker: str = ""

@dataclass
class ArbAlert:
    poly_market: PolyMarket
    odds_event: OddsEvent
    poly_side: str
    bet_team: str
    poly_price: float
    book_odds: float
    book_implied: float
    bookmaker: str
    edge: float

# ─── MATCHING ENGINE ──────────────────────────────────────────────────────────
_NOISE = {
    "the", "will", "win", "wins", "beat", "beats", "vs", "v",
    "fc", "united", "city", "real", "sporting", "athletic",
    "nba", "nhl", "nfl", "mlb", "epl", "atp", "wta",
    "western", "eastern", "conference", "finals", "final",
    "champion", "champions", "league", "cup", "series",
    "playoff", "playoffs", "game", "match", "season",
    "2026", "2025", "2024", "st", "san", "los", "las",
    "new", "york", "de", "la", "el", "ac", "sc", "if", "cf",
    "open", "masters", "grand", "slam",
}

_FUTURES_KW = [
    "win the nba", "win the nhl", "win the nfl", "win the mlb",
    "win the epl", "win the league", "win the cup", "win the title",
    "win la liga", "win bundesliga", "win serie a", "win ligue",
    "championship", "league winner", "title winner",
    "most valuable", "mvp", "award",
    "total wins", "season wins", "first to", "last to",
    "reach the finals", "advance to", "qualify",
    "will win the", "to win the",
]

def _sig(text: str) -> set[str]:
    words = text.lower().replace("?","").replace(".","").replace("-"," ").split()
    return {w for w in words if w not in _NOISE and len(w) > 2}

def _team_matches(team: str, question: str) -> bool:
    q = question.lower()
    t = team.lower()
    if t in q:
        return True
    sig = _sig(t)
    return bool(sig) and all(w in q for w in sig)

def _is_h2h(question: str) -> bool:
    q = question.lower()
    return not any(kw in q for kw in _FUTURES_KW)

def teams_match(team_a: str, team_b: str, question: str) -> bool:
    return _is_h2h(question) and _team_matches(team_a, question) and _team_matches(team_b, question)

# ─── POLYMARKET CLIENT ────────────────────────────────────────────────────────
class PolymarketClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_markets(self) -> list[PolyMarket]:
        all_raw = []
        offset = 0
        while offset < 1000:
            try:
                async with self.session.get(
                    f"{GAMMA_API_BASE}/markets",
                    params={"active": "true", "closed": "false", "limit": 100, "offset": offset},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        break
                    raw = await resp.json()
                    if not raw:
                        break
                    all_raw.extend(raw)
                    if len(raw) < 100:
                        break
                    offset += 100
            except Exception as e:
                logger.error(f"Polymarket erro: {e}")
                break

        SPORT_KW = [
            "vs", " v ", "wins", "beats",
            "nba", "nhl", "nfl", "mlb",
            "premier league", "la liga", "bundesliga", "serie a", "ligue",
            "champions league", "ucl", "uel", "europa",
            "atp", "wta", "tennis",
            "cs2", "counter-strike", "mongolz", "navi", "vitality", "faze",
            "dota", "valorant", "league of legends",
            "playoff", "semifinal",
        ]
        sport_raw = [
            m for m in all_raw
            if any(kw in (m.get("question") or "").lower() for kw in SPORT_KW)
        ]
        logger.info(f"Polymarket: {len(all_raw)} total → {len(sport_raw)} desportivos")

        markets = []
        for m in sport_raw:
            try:
                op = m.get("outcomePrices")
                if isinstance(op, str):
                    op = json.loads(op)
                if isinstance(op, list) and len(op) >= 2:
                    yes_price, no_price = float(op[0]), float(op[1])
                else:
                    yes_price = float(m.get("bestAsk") or m.get("lastTradePrice") or 0)
                    no_price = round(1 - yes_price, 4) if yes_price > 0 else 0
                if yes_price <= 0 or no_price <= 0:
                    continue
                slug = m.get("slug", str(m.get("id", "")))
                markets.append(PolyMarket(
                    condition_id=str(m.get("conditionId", m.get("id", ""))),
                    question=m.get("question", ""),
                    yes_price=yes_price,
                    no_price=no_price,
                    url=f"https://polymarket.com/event/{slug}",
                ))
            except Exception:
                continue

        logger.info(f"Polymarket: {len(markets)} mercados válidos")
        return markets

# ─── ODDSPAPI CLIENT ─────────────────────────────────────────────────────────
class OddsPapiClient:
    """
    Cobre: Futebol, Ténis, Esports (CS2, Dota2, LoL, Valorant)
    Usa odds-by-tournaments para futebol/ténis (eficiente)
    Usa fixtures+odds para esports (necessário pois não há torneio fixo)
    """
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    def _extract_price(self, outcomes: dict, outcome_id: str) -> float:
        try:
            return float(outcomes[outcome_id]["players"]["0"]["price"])
        except Exception:
            return 0.0

    async def _fetch_by_tournaments(
        self, tournament_ids: list[int], sport_label: str,
        market_home: str, market_away: str, market_draw: str = None
    ) -> list[OddsEvent]:
        """Busca odds de múltiplos torneios numa só chamada API."""
        if not ODDSPAPI_KEY or not tournament_ids:
            return []

        events = []
        # Dividir em chunks de 10 (limite da API)
        for i in range(0, len(tournament_ids), 10):
            chunk = tournament_ids[i:i+10]
            ids_str = ",".join(str(t) for t in chunk)
            try:
                async with self.session.get(
                    f"{ODDSPAPI_BASE}/odds-by-tournaments",
                    params={
                        "apiKey": ODDSPAPI_KEY,
                        "bookmaker": "pinnacle",
                        "tournamentIds": ids_str,
                        "oddsFormat": "decimal",
                    },
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"OddsPapi {sport_label} HTTP {resp.status}")
                        continue
                    data = await resp.json()

                if not isinstance(data, list):
                    continue

                for fix in data:
                    bk_odds = fix.get("bookmakerOdds", {})
                    # Tentar Pinnacle primeiro, depois qualquer outro
                    bk = bk_odds.get("pinnacle") or next(iter(bk_odds.values()), None)
                    if not bk:
                        continue

                    mkts = bk.get("markets", {})

                    # Extrair nomes das equipas a partir dos fixtures
                    team1 = fix.get("participant1Name") or fix.get("home_team", "")
                    team2 = fix.get("participant2Name") or fix.get("away_team", "")
                    if not team1 or not team2:
                        continue

                    # Extrair odds
                    home_price, away_price, draw_price = 0.0, 0.0, 0.0
                    home_book = away_book = draw_book = "Pinnacle"

                    if market_home in mkts:
                        outcomes = mkts[market_home].get("outcomes", {})
                        home_key = list(outcomes.keys())[0] if outcomes else None
                        away_key = list(outcomes.keys())[1] if len(outcomes) > 1 else None
                        draw_key = list(outcomes.keys())[2] if len(outcomes) > 2 else None

                        if home_key:
                            home_price = self._extract_price(outcomes, home_key)
                        if away_key:
                            away_price = self._extract_price(outcomes, away_key)
                        if draw_key and market_draw:
                            draw_price = self._extract_price(outcomes, draw_key)

                    if home_price <= 1 or away_price <= 1:
                        continue

                    # Para ténis — tentar também outros bookmakers para melhores odds
                    best_home, best_away = home_price, away_price
                    best_home_bk = best_away_bk = "Pinnacle"

                    for bk_slug, bk_data in bk_odds.items():
                        m = bk_data.get("markets", {})
                        if market_home not in m:
                            continue
                        ocs = m[market_home].get("outcomes", {})
                        keys = list(ocs.keys())
                        if len(keys) >= 2:
                            p1 = self._extract_price(ocs, keys[0])
                            p2 = self._extract_price(ocs, keys[1])
                            if p1 > best_home:
                                best_home, best_home_bk = p1, bk_slug
                            if p2 > best_away:
                                best_away, best_away_bk = p2, bk_slug

                    events.append(OddsEvent(
                        event_id=fix.get("fixtureId", ""),
                        sport=sport_label,
                        home_team=team1,
                        away_team=team2,
                        best_odds_home=best_home,
                        best_odds_away=best_away,
                        best_bookmaker_home=best_home_bk,
                        best_bookmaker_away=best_away_bk,
                        draw_odds=draw_price,
                        draw_bookmaker="Pinnacle",
                    ))
            except Exception as e:
                logger.error(f"OddsPapi {sport_label} chunk erro: {e}")

        return events

    async def get_football_events(self) -> list[OddsEvent]:
        """Futebol — mercado 1x2: outcome 101=home, 102=draw, 103=away"""
        events = await self._fetch_by_tournaments(
            FOOTBALL_TOURNAMENT_IDS, "football",
            market_home="101", market_away="103", market_draw="101"
        )
        logger.info(f"OddsPapi Futebol: {len(events)} jogos")
        return events

    async def get_tennis_events(self) -> list[OddsEvent]:
        """Ténis — mercado h2h: outcome 101=p1, 102=p2 (sem draw)"""
        events = await self._fetch_by_tournaments(
            TENNIS_TOURNAMENT_IDS, "tennis",
            market_home="101", market_away="101"
        )
        logger.info(f"OddsPapi Ténis: {len(events)} jogos")
        return events

    async def get_esport_events(self) -> list[OddsEvent]:
        """Esports — fixture por fixture (CS2, Dota2, LoL, Valorant)"""
        if not ODDSPAPI_KEY:
            logger.info("OddsPapi: sem key — esports indisponível")
            return []

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        week  = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
        events = []

        for sport_id, sport_name in ESPORT_IDS.items():
            try:
                async with self.session.get(
                    f"{ODDSPAPI_BASE}/fixtures",
                    params={"apiKey": ODDSPAPI_KEY, "sportId": sport_id,
                            "from": today, "to": week, "hasOdds": True, "statusId": "0,1,2"},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        continue
                    fixtures = await resp.json()
                    if not isinstance(fixtures, list):
                        continue

                for fix in fixtures:
                    fix_id = fix.get("fixtureId")
                    team1  = fix.get("participant1Name", "")
                    team2  = fix.get("participant2Name", "")
                    if not fix_id or not team1 or not team2:
                        continue

                    async with self.session.get(
                        f"{ODDSPAPI_BASE}/odds",
                        params={"apiKey": ODDSPAPI_KEY, "fixtureId": fix_id},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp2:
                        if resp2.status != 200:
                            continue
                        od = await resp2.json()

                    best1, book1 = 0.0, ""
                    best2, book2 = 0.0, ""
                    for bk_slug, bk_data in od.get("bookmakerOdds", {}).items():
                        m = bk_data.get("markets", {})
                        if "171" not in m:
                            continue
                        ocs = m["171"].get("outcomes", {})
                        p1 = ocs.get("171", {}).get("players", {}).get("0", {}).get("price")
                        p2 = ocs.get("172", {}).get("players", {}).get("0", {}).get("price")
                        if p1 and float(p1) > best1:
                            best1, book1 = float(p1), bk_slug
                        if p2 and float(p2) > best2:
                            best2, book2 = float(p2), bk_slug

                    if best1 <= 1 or best2 <= 1:
                        continue
                    events.append(OddsEvent(
                        event_id=str(fix_id), sport=sport_name,
                        home_team=team1, away_team=team2,
                        best_odds_home=best1, best_odds_away=best2,
                        best_bookmaker_home=book1, best_bookmaker_away=book2,
                    ))
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"OddsPapi esports ({sport_name}): {e}")

        logger.info(f"OddsPapi Esports: {len(events)} jogos")
        return events

# ─── THE ODDS API (NBA, NHL, NFL, MLB) ────────────────────────────────────────
class OddsAPIClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    def _best_odds(self, bookmakers, team_name):
        best, book = 0.0, ""
        for bm in bookmakers:
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue
                for oc in mkt.get("outcomes", []):
                    if oc["name"] == team_name and float(oc["price"]) > best:
                        best, book = float(oc["price"]), bm["title"]
        return best, book

    async def get_events(self) -> list[OddsEvent]:
        events = []
        for sport_key in TRADITIONAL_SPORT_KEYS:
            try:
                async with self.session.get(
                    f"{ODDS_API_BASE}/sports/{sport_key}/odds",
                    params={"apiKey": ODDS_API_KEY, "regions": "eu,uk,us",
                            "markets": "h2h", "oddsFormat": "decimal"},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                for ev in data:
                    home, away = ev["home_team"], ev["away_team"]
                    bms = ev.get("bookmakers", [])
                    if not bms:
                        continue
                    oh, bh = self._best_odds(bms, home)
                    oa, ba = self._best_odds(bms, away)
                    if oh <= 1 or oa <= 1:
                        continue
                    events.append(OddsEvent(
                        event_id=ev["id"], sport=sport_key,
                        home_team=home, away_team=away,
                        best_odds_home=oh, best_odds_away=oa,
                        best_bookmaker_home=bh, best_bookmaker_away=ba,
                    ))
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.error(f"OddsAPI ({sport_key}): {e}")

        logger.info(f"The Odds API: {len(events)} eventos")
        return events

# ─── FIND OPPORTUNITIES ───────────────────────────────────────────────────────
def find_opportunities(poly_markets: list[PolyMarket], all_events: list[OddsEvent]) -> list[ArbAlert]:
    alerts = []
    matched = 0

    for poly in poly_markets:
        for ev in all_events:
            if not teams_match(ev.home_team, ev.away_team, poly.question):
                continue
            matched += 1

            q = poly.question.lower()
            pos_h = min((q.find(w) for w in _sig(ev.home_team) if w in q), default=999)
            pos_a = min((q.find(w) for w in _sig(ev.away_team) if w in q), default=999)
            yes_is_home = pos_h <= pos_a

            checks = [
                ("YES", poly.yes_price,
                 ev.home_team if yes_is_home else ev.away_team,
                 ev.best_odds_home if yes_is_home else ev.best_odds_away,
                 ev.best_bookmaker_home if yes_is_home else ev.best_bookmaker_away),
                ("NO",  poly.no_price,
                 ev.away_team if yes_is_home else ev.home_team,
                 ev.best_odds_away if yes_is_home else ev.best_odds_home,
                 ev.best_bookmaker_away if yes_is_home else ev.best_bookmaker_home),
            ]

            for poly_side, poly_price, bet_team, book_odds, bookmaker in checks:
                if book_odds <= 1.0:
                    continue
                book_implied = 1.0 / book_odds
                net_edge = (book_implied - poly_price) - POLY_FEE
                if net_edge < MIN_EDGE:
                    continue
                alerts.append(ArbAlert(
                    poly_market=poly, odds_event=ev,
                    poly_side=poly_side, bet_team=bet_team,
                    poly_price=poly_price, book_odds=book_odds,
                    book_implied=book_implied, bookmaker=bookmaker,
                    edge=net_edge,
                ))

    logger.info(f"Matches: {matched} | Oportunidades: {len(alerts)}")
    alerts.sort(key=lambda a: a.edge, reverse=True)
    return alerts

# ─── TELEGRAM ────────────────────────────────────────────────────────────────
class Telegram:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def send(self, text: str):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            print("\n" + text + "\n")
            return
        try:
            async with self.session.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Telegram {resp.status}")
        except Exception as e:
            logger.error(f"Telegram: {e}")

    def format_alert(self, alert: ArbAlert) -> str:
        EMOJI = {
            "football": "⚽", "tennis": "🎾",
            "basketball_nba": "🏀", "icehockey_nhl": "🏒",
            "americanfootball_nfl": "🏈", "baseball_mlb": "⚾",
            "CS2": "🎮", "Dota 2": "🎮", "League of Legends": "🎮",
            "Valorant": "🎮", "Call of Duty": "🎮",
        }
        emoji = EMOJI.get(alert.odds_event.sport, "🏆")
        sport_label = alert.odds_event.sport.replace("_", " ").upper()

        return (
            f"🚨 <b>OPORTUNIDADE ARB</b> {emoji}\n"
            f"<i>{sport_label}</i>\n\n"
            f"📊 <b>Polymarket</b>\n"
            f"  ❓ {alert.poly_market.question}\n"
            f"  🎯 Apostar <b>{alert.poly_side}</b> @ <b>{alert.poly_price:.3f}</b> ({alert.poly_price:.1%})\n"
            f"  🔗 {alert.poly_market.url}\n\n"
            f"📈 <b>{alert.bookmaker}</b>\n"
            f"  ⚔️ {alert.odds_event.home_team} vs {alert.odds_event.away_team}\n"
            f"  🎯 Apostar <b>{alert.bet_team}</b> @ <b>{alert.book_odds:.2f}</b>\n"
            f"  📉 Implícito: {alert.book_implied:.1%}\n\n"
            f"💹 <b>Edge: +{alert.edge:.2%}</b>  "
            f"(Book {alert.book_implied:.1%} vs Poly {alert.poly_price:.1%})\n"
            f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
class ArbBot:
    def __init__(self):
        # key -> (edge_registado, timestamp_ultimo_alerta)
        self.seen: dict[str, tuple[float, float]] = {}

    def _key(self, a: ArbAlert) -> str:
        return f"{a.poly_market.condition_id}_{a.odds_event.event_id}_{a.poly_side}"

    def _should_alert(self, key: str, new_edge: float) -> bool:
        import time
        now = time.time()
        if key not in self.seen:
            return True
        old_edge, last_ts = self.seen[key]
        # Realertar se edge melhorou >1.5% OU passaram >5 minutos
        if new_edge - old_edge >= 0.015:
            return True
        if now - last_ts >= 300:
            return True
        return False

    async def run(self):
        logger.info("🚀 Bot iniciado")
        async with aiohttp.ClientSession() as session:
            poly     = PolymarketClient(session)
            oddspapi = OddsPapiClient(session)
            oddsapi  = OddsAPIClient(session)
            tg       = Telegram(session)

            esports_ok = "✅ Esports (CS2/Dota2/LoL/Valorant)" if ODDSPAPI_KEY else "⚠️ Esports: adiciona ODDSPAPI_KEY"
            football_ok = "✅ Futebol (EPL/UCL/LaLiga...)" if ODDSPAPI_KEY else "⚠️ Futebol: adiciona ODDSPAPI_KEY"
            tennis_ok = "✅ Ténis (ATP/WTA)" if ODDSPAPI_KEY else "⚠️ Ténis: adiciona ODDSPAPI_KEY"

            await tg.send(
                f"🤖 <b>Arbitrage Monitor Iniciado</b>\n\n"
                f"{football_ok}\n"
                f"{tennis_ok}\n"
                f"{esports_ok}\n"
                f"✅ NBA / NHL / NFL / MLB\n\n"
                f"💹 Edge mínimo: {MIN_EDGE:.0%} | Fee Poly: {POLY_FEE:.0%}\n"
                f"🔄 Scan a cada {SCAN_INTERVAL}s"
            )

            while True:
                try:
                    logger.info("🔍 A procurar oportunidades...")

                    # Correr todas as fontes em paralelo
                    poly_markets, football, tennis, esports, traditional = await asyncio.gather(
                        poly.get_markets(),
                        oddspapi.get_football_events(),
                        oddspapi.get_tennis_events(),
                        oddspapi.get_esport_events(),
                        oddsapi.get_events(),
                        return_exceptions=True
                    )

                    # Filtrar erros
                    all_events = []
                    for result in [football, tennis, esports, traditional]:
                        if isinstance(result, list):
                            all_events.extend(result)
                        else:
                            logger.error(f"Fonte com erro: {result}")

                    if not isinstance(poly_markets, list):
                        logger.error(f"Polymarket erro: {poly_markets}")
                        poly_markets = []

                    opportunities = find_opportunities(poly_markets, all_events)

                    import time
                    new = 0
                    for alert in opportunities:
                        key = self._key(alert)
                        if not self._should_alert(key, alert.edge):
                            continue
                        self.seen[key] = (alert.edge, time.time())
                        new += 1
                        await tg.send(tg.format_alert(alert))
                        await asyncio.sleep(1)

                    if new == 0:
                        logger.info("Nenhuma oportunidade nova")
                    else:
                        logger.info(f"✅ {new} alertas enviados")

                    if len(self.seen) > 2000:
                        self.seen.clear()

                except Exception as e:
                    logger.error(f"Erro no loop: {e}", exc_info=True)
                    await tg.send(f"⚠️ Erro: {e}")

                await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(ArbBot().run())
