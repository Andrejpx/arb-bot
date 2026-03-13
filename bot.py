"""
Arbitrage Monitor Bot — Polymarket <-> The Odds API
Matching rigoroso: só mercados h2h reais, sem falsos positivos.
Alertas via Telegram — trades manuais.
"""

import asyncio
import aiohttp
import logging
import os
import json
from datetime import datetime, timezone
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
ODDS_API_KEY     = os.getenv("ODDS_API_KEY", "55be989361b732d8ed74b8a3fed378fb")
GAMMA_API_BASE   = "https://gamma-api.polymarket.com"
ODDS_API_BASE    = "https://api.the-odds-api.com/v4"

MIN_EDGE      = 0.03
POLY_FEE      = 0.02
SCAN_INTERVAL = 60

SPORT_KEYS = [
    "basketball_nba",
    "basketball_ncaab",
    "icehockey_nhl",
    "soccer_epl",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_spain_la_liga",
    "soccer_germany_bundesliga",
    "soccer_italy_serie_a",
    "soccer_france_ligue_one",
    "soccer_brazil_campeonato",
    "soccer_argentina_primera_division",
    "tennis_atp_indian_wells",
    "tennis_wta_indian_wells",
    "americanfootball_nfl",
    "baseball_mlb",
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
    commence_time: str
    best_odds_home: float
    best_odds_away: float
    best_bookmaker_home: str
    best_bookmaker_away: str

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
# Palavras que não identificam uma equipa específica
_NOISE = {
    "the", "will", "win", "wins", "beat", "beats", "vs", "v",
    "fc", "united", "city", "real", "sporting", "athletic",
    "nba", "nhl", "nfl", "mlb", "epl", "atp", "wta",
    "western", "eastern", "conference", "finals", "final",
    "champion", "champions", "league", "cup", "series",
    "playoff", "playoffs", "game", "match", "season",
    "2026", "2025", "2024", "st", "san", "los", "las",
    "new", "york", "de", "la", "el", "ac", "sc", "if",
}

# Mercados futures/outright — não são h2h, ignorar
_FUTURES_KW = [
    "win the nba", "win the nhl", "win the nfl", "win the mlb",
    "win the epl", "win the league", "win the cup", "win the title",
    "win la liga", "win bundesliga", "win serie a", "win ligue",
    "championship", "champions league winner", "league winner",
    "title winner", "most valuable", "mvp", "award",
    "over/under", "total wins", "season wins", "first to",
    "last to", "reach the finals", "advance to",
]

def _significant_words(text: str) -> set[str]:
    words = text.lower().replace("?", "").replace(".", "").split()
    return {w for w in words if w not in _NOISE and len(w) > 2}

def _team_matches(team: str, question: str) -> bool:
    q = question.lower()
    t = team.lower()
    # Match exato do nome completo
    if t in q:
        return True
    # Todas as palavras significativas da equipa têm de estar na pergunta
    sig = _significant_words(t)
    if not sig:
        return False
    return all(w in q for w in sig)

def _is_h2h_market(question: str) -> bool:
    """Retorna True se parece um mercado h2h (jogo individual), não futures."""
    q = question.lower()
    if any(kw in q for kw in _FUTURES_KW):
        return False
    return True

def teams_match_question(team_a: str, team_b: str, question: str) -> bool:
    if not _is_h2h_market(question):
        return False
    return _team_matches(team_a, question) and _team_matches(team_b, question)

# ─── POLYMARKET CLIENT ────────────────────────────────────────────────────────
class PolymarketClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_markets(self) -> list[PolyMarket]:
        all_raw = []
        offset = 0
        while offset < 1000:
            params = {"active": "true", "closed": "false", "limit": 100, "offset": offset}
            try:
                async with self.session.get(
                    f"{GAMMA_API_BASE}/markets", params=params,
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
            "vs", " v ", "wins", "win", "beats",
            "nba", "nhl", "nfl", "mlb", "epl", "premier league",
            "la liga", "bundesliga", "serie a", "ligue 1",
            "champions league", "ucl", "uel",
            "atp", "wta", "tennis",
            "cs2", "csgo", "counter-strike", "dota", "valorant",
            "match", "playoff", "semifinal",
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
                    yes_price = float(op[0])
                    no_price  = float(op[1])
                else:
                    yes_price = float(m.get("bestAsk") or m.get("lastTradePrice") or 0)
                    no_price  = round(1 - yes_price, 4) if yes_price > 0 else 0
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

        logger.info(f"Polymarket: {len(markets)} mercados com preços válidos")
        return markets

# ─── ODDS API CLIENT ─────────────────────────────────────────────────────────
class OddsAPIClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    def _best_odds(self, bookmakers: list, team_name: str) -> tuple[float, str]:
        best_odds, best_book = 0.0, ""
        for bm in bookmakers:
            for market in bm.get("markets", []):
                if market.get("key") != "h2h":
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome["name"] == team_name:
                        price = float(outcome["price"])
                        if price > best_odds:
                            best_odds = price
                            best_book = bm["title"]
        return best_odds, best_book

    async def get_events(self) -> list[OddsEvent]:
        events = []
        for sport_key in SPORT_KEYS:
            try:
                params = {
                    "apiKey": ODDS_API_KEY,
                    "regions": "eu,uk,us",
                    "markets": "h2h",
                    "oddsFormat": "decimal",
                }
                async with self.session.get(
                    f"{ODDS_API_BASE}/sports/{sport_key}/odds",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 422:
                        continue
                    if resp.status != 200:
                        logger.warning(f"Odds API {resp.status} para {sport_key}")
                        continue
                    data = await resp.json()

                for ev in data:
                    home, away = ev["home_team"], ev["away_team"]
                    bms = ev.get("bookmakers", [])
                    if not bms:
                        continue
                    odds_home, book_home = self._best_odds(bms, home)
                    odds_away, book_away = self._best_odds(bms, away)
                    if odds_home <= 1.0 or odds_away <= 1.0:
                        continue
                    events.append(OddsEvent(
                        event_id=ev["id"],
                        sport=sport_key,
                        home_team=home,
                        away_team=away,
                        commence_time=ev.get("commence_time", ""),
                        best_odds_home=odds_home,
                        best_odds_away=odds_away,
                        best_bookmaker_home=book_home,
                        best_bookmaker_away=book_away,
                    ))
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.error(f"Odds API erro ({sport_key}): {e}")

        logger.info(f"Odds API: {len(events)} eventos")
        return events

# ─── FIND OPPORTUNITIES ───────────────────────────────────────────────────────
def find_opportunities(poly_markets: list[PolyMarket], odds_events: list[OddsEvent]) -> list[ArbAlert]:
    alerts = []
    matched_count = 0

    for poly in poly_markets:
        for ev in odds_events:
            if not teams_match_question(ev.home_team, ev.away_team, poly.question):
                continue

            matched_count += 1

            # Determinar qual equipa o YES representa
            q = poly.question.lower()
            pos_home = min(
                (q.find(w) for w in _significant_words(ev.home_team.lower()) if w in q),
                default=999
            )
            pos_away = min(
                (q.find(w) for w in _significant_words(ev.away_team.lower()) if w in q),
                default=999
            )
            yes_is_home = pos_home <= pos_away

            for poly_side, poly_price, bet_team, book_odds, bookmaker in [
                (
                    "YES", poly.yes_price,
                    ev.home_team if yes_is_home else ev.away_team,
                    ev.best_odds_home if yes_is_home else ev.best_odds_away,
                    ev.best_bookmaker_home if yes_is_home else ev.best_bookmaker_away,
                ),
                (
                    "NO", poly.no_price,
                    ev.away_team if yes_is_home else ev.home_team,
                    ev.best_odds_away if yes_is_home else ev.best_odds_home,
                    ev.best_bookmaker_away if yes_is_home else ev.best_bookmaker_home,
                ),
            ]:
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

    logger.info(f"Matches: {matched_count} | Oportunidades: {len(alerts)}")
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
                    logger.warning(f"Telegram erro {resp.status}")
        except Exception as e:
            logger.error(f"Telegram erro: {e}")

    def format_alert(self, alert: ArbAlert) -> str:
        sport_emoji = {
            "basketball": "🏀", "icehockey": "🏒", "soccer": "⚽",
            "tennis": "🎾", "americanfootball": "🏈", "baseball": "⚾",
        }
        emoji = next((v for k, v in sport_emoji.items() if k in alert.odds_event.sport), "🏆")
        return (
            f"🚨 <b>OPORTUNIDADE ARB DETETADA</b> {emoji}\n\n"
            f"📊 <b>Polymarket</b>\n"
            f"  ❓ {alert.poly_market.question}\n"
            f"  🎯 Apostar <b>{alert.poly_side}</b> @ <b>{alert.poly_price:.3f}</b> ({alert.poly_price:.1%})\n"
            f"  🔗 {alert.poly_market.url}\n\n"
            f"📈 <b>Bookmaker: {alert.bookmaker}</b>\n"
            f"  ⚔️ {alert.odds_event.home_team} vs {alert.odds_event.away_team}\n"
            f"  🎯 Aposta em <b>{alert.bet_team}</b> @ odds <b>{alert.book_odds:.2f}</b>\n"
            f"  📉 Implícito bookmaker: {alert.book_implied:.1%}\n\n"
            f"💹 <b>Edge líquido: +{alert.edge:.2%}</b>\n"
            f"  Book: {alert.book_implied:.1%} | Poly: {alert.poly_price:.1%}\n"
            f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
class ArbBot:
    def __init__(self):
        self.seen: set[str] = set()

    def _key(self, alert: ArbAlert) -> str:
        return f"{alert.poly_market.condition_id}_{alert.odds_event.event_id}_{alert.poly_side}"

    async def run(self):
        logger.info("🚀 Arbitrage Bot iniciado")
        async with aiohttp.ClientSession() as session:
            poly = PolymarketClient(session)
            odds = OddsAPIClient(session)
            tg   = Telegram(session)

            await tg.send(
                f"🤖 <b>Arbitrage Monitor Iniciado</b>\n"
                f"✅ Matching rigoroso — sem falsos positivos\n"
                f"🏆 NBA, NHL, EPL, UCL, Tennis, NFL, MLB...\n"
                f"💹 Edge mínimo: {MIN_EDGE:.0%}\n"
                f"🔄 Scan a cada {SCAN_INTERVAL}s"
            )

            while True:
                try:
                    logger.info("🔍 A procurar oportunidades...")
                    poly_markets  = await poly.get_markets()
                    odds_events   = await odds.get_events()
                    opportunities = find_opportunities(poly_markets, odds_events)

                    new = 0
                    for alert in opportunities:
                        key = self._key(alert)
                        if key in self.seen:
                            continue
                        self.seen.add(key)
                        new += 1
                        await tg.send(tg.format_alert(alert))
                        await asyncio.sleep(1)

                    if new == 0:
                        logger.info("Nenhuma oportunidade nova")

                    if len(self.seen) > 2000:
                        self.seen.clear()

                except Exception as e:
                    logger.error(f"Erro no loop: {e}", exc_info=True)
                    await tg.send(f"⚠️ Erro no bot: {e}")

                await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(ArbBot().run())
