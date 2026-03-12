"""
Arbitrage Monitor Bot — Polymarket <-> CSGOEmpire
Compara odds entre as duas plataformas e envia alertas via Telegram.
Sem execução automática — trades feitos manualmente.
"""

import asyncio
import aiohttp
import logging
import os
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
POLYMARKET_API_BASE = "https://clob.polymarket.com"
CSGOEMPIRE_API_BASE = "https://csgoempire.com/api/v2"
CSGOEMPIRE_API_KEY  = os.getenv("CSGOEMPIRE_API_KEY", "")

MIN_POLY_VOLUME     = float(os.getenv("MIN_POLY_VOLUME", "10000"))
MIN_POLY_LIQUIDITY  = float(os.getenv("MIN_POLY_LIQUIDITY", "10000"))
MIN_EDGE            = 0.04
POLY_FEE            = 0.02
EMPIRE_FEE          = 0.05
SCAN_INTERVAL       = 30

# ─── DATA CLASSES ─────────────────────────────────────────────────────────────
@dataclass
class PolyMarket:
    condition_id: str
    question: str
    yes_price: float
    no_price: float
    volume: float
    liquidity: float
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
def _get_float(m: dict, *keys) -> float:
    """Tenta múltiplos nomes de campo e retorna o primeiro valor válido."""
    for k in keys:
        v = m.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return 0.0

class PolymarketClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_markets(self) -> list[PolyMarket]:
        markets = []
        page = 0
        diagnosed = False

        while page < 5:
            page += 1
            params = {"active": "true", "closed": "false", "limit": 100}
            if page > 1:
                params["offset"] = (page - 1) * 100

            try:
                async with self.session.get(
                    f"{POLYMARKET_API_BASE}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10, connect=5)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Polymarket HTTP {resp.status}")
                        break
                    data = await resp.json()
            except asyncio.TimeoutError:
                logger.error("Polymarket timeout")
                break
            except Exception as e:
                logger.error(f"Polymarket erro: {e}")
                break

            raw = data.get("data", [])
            if not raw:
                break

            # Log diagnóstico do primeiro mercado para perceber os campos disponíveis
            if not diagnosed and raw:
                diagnosed = True
                sample = raw[0]
                vol_fields = {k: v for k, v in sample.items()
                              if any(x in k.lower() for x in ["vol", "liq", "price", "amount"])}
                logger.info(f"Polymarket campos de valor: {vol_fields}")
                logger.info(f"Polymarket todos os campos: {list(sample.keys())}")

            for m in raw:
                try:
                    # Testar múltiplos nomes de campo possíveis
                    volume    = _get_float(m, "volume", "volumeNum", "volume24hr",
                                           "usd_volume", "total_volume", "notional_volume")
                    liquidity = _get_float(m, "liquidity", "liquidityNum", "open_interest",
                                           "usd_liquidity", "total_liquidity")

                    if volume < MIN_POLY_VOLUME or liquidity < MIN_POLY_LIQUIDITY:
                        continue

                    tokens    = m.get("tokens", [])
                    yes_token = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), None)
                    no_token  = next((t for t in tokens if t.get("outcome", "").upper() == "NO"), None)
                    if not yes_token or not no_token:
                        continue

                    yes_price = _get_float(yes_token, "price")
                    no_price  = _get_float(no_token, "price")
                    if yes_price <= 0 or no_price <= 0:
                        continue

                    slug = m.get("market_slug", m.get("slug", m.get("condition_id", "")))
                    markets.append(PolyMarket(
                        condition_id=m.get("condition_id", m.get("id", "")),
                        question=m.get("question", m.get("title", "")),
                        yes_price=yes_price,
                        no_price=no_price,
                        volume=volume,
                        liquidity=liquidity,
                        url=f"https://polymarket.com/event/{slug}",
                    ))
                except Exception as e:
                    logger.debug(f"Mercado ignorado: {e}")
                    continue

            next_cursor = data.get("next_cursor", "")
            if not next_cursor or next_cursor in ("LTE=", ""):
                break

        logger.info(f"Polymarket: {len(markets)} mercados elegíveis (vol>={MIN_POLY_VOLUME}, liq>={MIN_POLY_LIQUIDITY})")
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
            EmpireEvent("1", "Manchester City", "Arsenal",    2.10, 3.50, 3.20, "football"),
            EmpireEvent("2", "Real Madrid",     "Barcelona",  2.40, 2.90, 3.10, "football"),
            EmpireEvent("3", "Vitality",        "NAVI",       1.85, 2.00, None, "csgo"),
            EmpireEvent("4", "FaZe",            "G2",         2.20, 1.70, None, "csgo"),
            EmpireEvent("5", "Lakers",          "Celtics",    2.05, 1.80, None, "basketball"),
            EmpireEvent("6", "PSG",             "Bayern",     2.60, 2.70, 3.30, "football"),
        ]

# ─── MATCHING ENGINE ──────────────────────────────────────────────────────────
def find_opportunities(poly_markets, empire_events) -> list[ArbAlert]:
    alerts = []
    for poly in poly_markets:
        q = poly.question.lower()
        for ev in empire_events:
            a = ev.team_a.lower()
            b = ev.team_b.lower()
            words_a = [w for w in a.split() if len(w) > 3]
            words_b = [w for w in b.split() if len(w) > 3]
            match_a = a in q or any(w in q for w in words_a)
            match_b = b in q or any(w in q for w in words_b)
            if not (match_a and match_b):
                continue

            pos_a = q.find(words_a[0]) if words_a and words_a[0] in q else 999
            pos_b = q.find(words_b[0]) if words_b and words_b[0] in q else 999
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
            f"  💰 Vol: <b>${alert.poly_market.volume:,.0f}</b> | Liq: <b>${alert.poly_market.liquidity:,.0f}</b>\n"
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
                f"📊 Vol/Liq mínimo: ${MIN_POLY_VOLUME:,.0f}\n"
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
