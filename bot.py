"""
Arbitrage Monitor Bot — Polymarket <-> CSGOEmpire
Só mercados de Sports e Esports — partidas individuais.
Alertas via Telegram — trades manuais.
"""

import asyncio
import aiohttp
import logging
import os
import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
CSGOEMPIRE_API_KEY  = os.getenv("CSGOEMPIRE_API_KEY", "")
GAMMA_API_BASE      = "https://gamma-api.polymarket.com"
CSGOEMPIRE_API_BASE = "https://csgoempire.com/api/v2"

MIN_EDGE      = 0.04
POLY_FEE      = 0.02
EMPIRE_FEE    = 0.05
SCAN_INTERVAL = 30

# Palavras-chave que identificam mercados desportivos
# Filtramos APÓS receber os dados, pela pergunta do mercado
SPORT_KEYWORDS = [
    # Equipas/jogadores — match direto
    "vs", " v ", "beats", "wins", "win",
    # Ligas e torneios
    "epl", "premier league", "la liga", "bundesliga", "serie a", "ligue 1",
    "champions league", "ucl", "uel", "nba", "nhl", "mls", "atp", "wta",
    "mlb", "nfl", "ufc",
    # Esports
    "cs2", "csgo", "counter-strike", "dota", "valorant", "league of legends",
    "call of duty", "rainbow six",
    # Desporto genérico
    "match", "game", "fixture", "tournament", "playoff", "semifinal", "final",
    "moneyline", "spread", "over", "under",
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
class EmpireEvent:
    match_id: str
    team_a: str
    team_b: str
    odds_a: float
    odds_b: float
    draw_odds: Optional[float]
    sport: str

@dataclass
class ArbAlert:
    poly_market: PolyMarket
    empire_event: EmpireEvent
    poly_side: str
    empire_side: str
    poly_price: float
    empire_odds: float
    empire_implied: float
    edge: float

# ─── POLYMARKET CLIENT ────────────────────────────────────────────────────────
class PolymarketClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_markets(self) -> list[PolyMarket]:
        all_raw = []
        offset = 0

        # Buscar mercados paginando até 1000
        while offset < 1000:
            params = {
                "active": "true",
                "closed": "false",
                "limit": 100,
                "offset": offset,
            }
            try:
                async with self.session.get(
                    f"{GAMMA_API_BASE}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10, connect=5)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Polymarket HTTP {resp.status}")
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

        logger.info(f"Polymarket: {len(all_raw)} mercados totais recebidos")

        # Filtrar por keywords desportivas
        sport_markets_raw = []
        for m in all_raw:
            q = (m.get("question") or m.get("title") or "").lower()
            if any(kw in q for kw in SPORT_KEYWORDS):
                sport_markets_raw.append(m)

        logger.info(f"Polymarket: {len(sport_markets_raw)} mercados desportivos após filtro")

        # Log exemplos para diagnóstico
        if sport_markets_raw:
            examples = [m.get("question", "") for m in sport_markets_raw[:10]]
            logger.info(f"Exemplos desportivos: {examples}")

        markets = []
        for m in sport_markets_raw:
            try:
                outcome_prices = m.get("outcomePrices")
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)

                if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                    yes_price = float(outcome_prices[0])
                    no_price  = float(outcome_prices[1])
                else:
                    yes_price = float(m.get("bestAsk") or m.get("lastTradePrice") or 0)
                    no_price  = round(1 - yes_price, 4) if yes_price > 0 else 0

                if yes_price <= 0 or no_price <= 0:
                    continue

                slug = m.get("slug", str(m.get("id", "")))
                markets.append(PolyMarket(
                    condition_id=str(m.get("conditionId", m.get("id", ""))),
                    question=m.get("question", m.get("title", "")),
                    yes_price=yes_price,
                    no_price=no_price,
                    url=f"https://polymarket.com/event/{slug}",
                ))
            except Exception as e:
                logger.debug(f"Mercado ignorado: {e}")

        logger.info(f"Polymarket: {len(markets)} mercados desportivos com preços válidos")
        return markets

# ─── CSGOEMPIRE CLIENT ────────────────────────────────────────────────────────
class CSGOEmpireClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_events(self) -> list[EmpireEvent]:
        if not CSGOEMPIRE_API_KEY:
            logger.info("CSGOEmpire: sem API key — a usar dados mock")
            return self._mock_events()

        headers = {"Authorization": f"Bearer {CSGOEMPIRE_API_KEY}"}
        try:
            async with self.session.get(
                f"{CSGOEMPIRE_API_BASE}/sports/events",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10, connect=5)
            ) as resp:
                if resp.status in (401, 403):
                    logger.warning(f"CSGOEmpire: sem acesso ({resp.status}) — a usar dados mock")
                    return self._mock_events()
                if resp.status != 200:
                    logger.warning(f"CSGOEmpire HTTP {resp.status} — a usar dados mock")
                    return self._mock_events()
                data = await resp.json()
        except Exception as e:
            logger.warning(f"CSGOEmpire erro ({e}) — a usar dados mock")
            return self._mock_events()

        events = []
        for e in data.get("data", []):
            try:
                odds   = e.get("odds", {})
                odds_a = float(odds.get("home", 0) or 0)
                odds_b = float(odds.get("away", 0) or 0)
                if odds_a <= 1 or odds_b <= 1:
                    continue
                events.append(EmpireEvent(
                    match_id=str(e["id"]),
                    team_a=e.get("home_team", "Team A"),
                    team_b=e.get("away_team", "Team B"),
                    odds_a=odds_a,
                    odds_b=odds_b,
                    draw_odds=float(odds["draw"]) if odds.get("draw") else None,
                    sport=e.get("sport", "").lower(),
                ))
            except Exception:
                continue

        logger.info(f"CSGOEmpire: {len(events)} eventos reais")
        return events

    def _mock_events(self) -> list[EmpireEvent]:
        return [
            EmpireEvent("1",  "Fluminense",        "Vasco da Gama",    2.80, 2.50, 3.20, "football"),
            EmpireEvent("2",  "Ferencvaros",        "Braga",            2.40, 2.90, 3.10, "football"),
            EmpireEvent("3",  "Nottingham Forest",  "Midtjylland",      1.80, 4.50, 3.80, "football"),
            EmpireEvent("4",  "Bologna",            "Roma",             2.60, 2.70, 3.30, "football"),
            EmpireEvent("5",  "Celta Vigo",         "Lyon",             2.20, 3.10, 3.00, "football"),
            EmpireEvent("6",  "Rhode Island",       "Duquesne",         1.50, 2.60, None, "basketball"),
            EmpireEvent("7",  "Nevada",             "Grand Canyon",     3.50, 1.35, None, "basketball"),
            EmpireEvent("8",  "Ohio",               "Kent State",       1.80, 2.10, None, "basketball"),
            EmpireEvent("9",  "Andreescu",          "Jones",            1.35, 3.20, None, "tennis"),
            EmpireEvent("10", "Svitolina",          "Swiatek",          1.65, 2.30, None, "tennis"),
            EmpireEvent("11", "Tien",               "Sinner",           3.80, 1.25, None, "tennis"),
            EmpireEvent("12", "Vitality",           "NAVI",             1.85, 2.00, None, "csgo"),
            EmpireEvent("13", "Aurora",             "Team Yandex",      2.10, 1.75, None, "dota2"),
            EmpireEvent("14", "Team Liquid",        "Tundra",           1.90, 1.95, None, "dota2"),
        ]

# ─── MATCHING ENGINE ──────────────────────────────────────────────────────────
def _team_in_question(team: str, question: str) -> bool:
    q = question.lower()
    t = team.lower()
    if t in q:
        return True
    words = [w for w in t.split() if len(w) > 3]
    return any(w in q for w in words)

def find_opportunities(poly_markets, empire_events) -> list[ArbAlert]:
    alerts = []
    matched = []

    for poly in poly_markets:
        q = poly.question.lower()
        for ev in empire_events:
            if not (_team_in_question(ev.team_a, poly.question) and
                    _team_in_question(ev.team_b, poly.question)):
                continue

            matched.append(poly.question)
            a_words = [w for w in ev.team_a.lower().split() if len(w) > 3]
            b_words = [w for w in ev.team_b.lower().split() if len(w) > 3]
            pos_a = next((q.find(w) for w in a_words if w in q), 999)
            pos_b = next((q.find(w) for w in b_words if w in q), 999)
            yes_is_team_a = pos_a <= pos_b

            for poly_side, poly_price, empire_side, empire_odds in [
                ("YES", poly.yes_price, "HOME", ev.odds_a if yes_is_team_a else ev.odds_b),
                ("NO",  poly.no_price,  "AWAY", ev.odds_b if yes_is_team_a else ev.odds_a),
            ]:
                if empire_odds <= 1:
                    continue
                empire_implied = 1.0 / empire_odds
                net_edge = (empire_implied - poly_price) - POLY_FEE - EMPIRE_FEE
                if net_edge < MIN_EDGE:
                    continue
                alerts.append(ArbAlert(
                    poly_market=poly, empire_event=ev,
                    poly_side=poly_side, empire_side=empire_side,
                    poly_price=poly_price, empire_odds=empire_odds,
                    empire_implied=empire_implied, edge=net_edge,
                ))

    if matched:
        logger.info(f"Mercados com match ({len(matched)}): {matched[:5]}")
    else:
        logger.info("Nenhum match — eventos mock não correspondem aos mercados atuais")

    alerts.sort(key=lambda a: a.edge, reverse=True)
    logger.info(f"Oportunidades encontradas: {len(alerts)}")
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
                    logger.warning(f"Telegram erro: {resp.status}")
        except Exception as e:
            logger.error(f"Telegram erro: {e}")

    def format_alert(self, alert: ArbAlert, is_mock: bool = False) -> str:
        mock_note = "\n⚠️ <i>Empire: dados de teste — verifica manualmente no site</i>" if is_mock else ""
        return (
            f"🚨 <b>OPORTUNIDADE DETECTADA</b>\n\n"
            f"📊 <b>Polymarket</b>\n"
            f"  ❓ {alert.poly_market.question}\n"
            f"  🎯 Apostar <b>{alert.poly_side}</b> @ <b>{alert.poly_price:.3f}</b> ({alert.poly_price:.1%})\n"
            f"  🔗 {alert.poly_market.url}\n\n"
            f"🎮 <b>CSGOEmpire</b>\n"
            f"  ⚔️ {alert.empire_event.team_a} vs {alert.empire_event.team_b}\n"
            f"  🎯 Apostar <b>{alert.empire_side}</b> @ odds <b>{alert.empire_odds:.2f}</b>\n"
            f"  📈 Probabilidade implícita: {alert.empire_implied:.1%}\n\n"
            f"💹 <b>Edge líquido: {alert.edge:.2%}</b>\n"
            f"  Empire vê {alert.empire_implied:.1%} | Poly cobra {alert.poly_price:.1%}"
            f"{mock_note}\n"
            f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
class ArbBot:
    def __init__(self):
        self.seen: set[str] = set()

    def _key(self, alert: ArbAlert) -> str:
        return f"{alert.poly_market.condition_id}_{alert.empire_event.match_id}_{alert.poly_side}"

    async def run(self):
        logger.info("🚀 Arbitrage Bot iniciado")
        async with aiohttp.ClientSession() as session:
            poly   = PolymarketClient(session)
            empire = CSGOEmpireClient(session)
            tg     = Telegram(session)

            await tg.send(
                f"🤖 <b>Arbitrage Monitor Iniciado</b>\n"
                f"⚠️ Modo: Só Alertas — trades manuais\n"
                f"🏆 Sports + Esports (filtro por keywords)\n"
                f"💹 Edge mínimo: {MIN_EDGE:.0%}\n"
                f"🔄 Scan a cada {SCAN_INTERVAL}s"
            )

            while True:
                try:
                    logger.info("🔍 A procurar oportunidades...")
                    poly_markets  = await poly.get_markets()
                    empire_events = await empire.get_events()
                    opportunities = find_opportunities(poly_markets, empire_events)
                    is_mock       = not bool(CSGOEMPIRE_API_KEY)

                    new = 0
                    for alert in opportunities:
                        key = self._key(alert)
                        if key in self.seen:
                            continue
                        self.seen.add(key)
                        new += 1
                        await tg.send(tg.format_alert(alert, is_mock=is_mock))
                        await asyncio.sleep(1)

                    if new == 0:
                        logger.info("Nenhuma oportunidade nova")

                    if len(self.seen) > 1000:
                        self.seen.clear()

                except Exception as e:
                    logger.error(f"Erro no loop: {e}", exc_info=True)
                    await tg.send(f"⚠️ Erro no bot: {e}")

                await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(ArbBot().run())
