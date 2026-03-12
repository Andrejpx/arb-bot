"""
Arbitrage Bot: Polymarket <-> CSGOEmpire
Monitoriza mercados de Desporto e Esports/CS2
Envia alertas via Telegram e executa trades automaticamente no CSGOEmpire
"""

import asyncio
import aiohttp
import logging
import json
import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("arb_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN       = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID")
CSGOEMPIRE_API_KEY   = os.getenv("CSGOEMPIRE_API_KEY")
POLYMARKET_API_BASE  = "https://clob.polymarket.com"
CSGOEMPIRE_API_BASE  = "https://csgoempire.com/api/v2"

MIN_POLY_VOLUME      = 10_000   # USD — mínimo de volume exigido
MIN_POLY_LIQUIDITY   = 10_000   # USD — mínimo de liquidez exigida
MIN_ARB_EDGE         = 0.04     # 4% edge mínimo após fees
POLY_FEE             = 0.02     # 2% fee Polymarket
EMPIRE_FEE           = 0.05     # 5% fee CSGOEmpire (ajusta conforme o teu tier)
SCAN_INTERVAL        = 60       # segundos entre scans
MAX_AUTO_BET_USD     = float(os.getenv("MAX_AUTO_BET_USD", "50"))  # cap por trade automático

# Tags de mercados a monitorizar (Polymarket usa tags/categorias)
TARGET_TAGS = ["sports", "esports", "cs2", "csgo", "football", "soccer", "nba", "basketball"]

# ─── DATA CLASSES ─────────────────────────────────────────────────────────────
@dataclass
class PolyMarket:
    condition_id: str
    question: str
    yes_price: float      # probabilidade implícita YES (0-1)
    no_price: float       # probabilidade implícita NO (0-1)
    volume: float
    liquidity: float
    end_date: Optional[str]
    tags: list = field(default_factory=list)

@dataclass
class EmpireEvent:
    match_id: str
    title: str
    team_a: str
    team_b: str
    odds_a: float         # odds decimais
    odds_b: float
    draw_odds: Optional[float]
    sport: str
    starts_at: Optional[str]

@dataclass
class ArbOpportunity:
    poly_market: PolyMarket
    empire_event: EmpireEvent
    poly_side: str        # "YES" ou "NO"
    empire_side: str      # "team_a", "team_b", "draw"
    poly_price: float     # preço no Polymarket (0-1)
    empire_implied: float # probabilidade implícita das odds do Empire
    edge: float           # edge após fees (%)
    recommended_stake_usd: float
    expected_profit_usd: float

# ─── POLYMARKET CLIENT ────────────────────────────────────────────────────────
class PolymarketClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_markets(self) -> list[PolyMarket]:
        """Busca mercados ativos com volume e liquidez mínimos."""
        markets = []
        next_cursor = None
        logger.info("A contactar Polymarket API...")

        while True:
            params = {
                "active": "true",
                "closed": "false",
                "limit": 100,
            }
            if next_cursor:
                params["next_cursor"] = next_cursor

            try:
                async with self.session.get(
                    f"{POLYMARKET_API_BASE}/markets", params=params,
                    timeout=aiohttp.ClientTimeout(total=10, connect=5)
                ) as resp:
                    logger.info(f"Polymarket resposta: HTTP {resp.status}")
                    if resp.status != 200:
                        logger.warning(f"Polymarket API error: {resp.status}")
                        break
                    data = await resp.json()
                    logger.info(f"Polymarket pagina recebida: {len(data.get('data', []))} mercados")
            except asyncio.TimeoutError:
                logger.error("Polymarket timeout — Railway pode estar a bloquear requests externos")
                break
            except Exception as e:
                logger.error(f"Polymarket fetch error: {type(e).__name__}: {e}")
                break

            raw_markets = data.get("data", [])
            for m in raw_markets:
                try:
                    volume    = float(m.get("volume", 0) or 0)
                    liquidity = float(m.get("liquidity", 0) or 0)

                    if volume < MIN_POLY_VOLUME or liquidity < MIN_POLY_LIQUIDITY:
                        continue

                    tags = [t.lower() for t in (m.get("tags") or [])]
                    if not any(t in TARGET_TAGS for t in tags):
                        continue

                    tokens = m.get("tokens", [])
                    yes_token = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), None)
                    no_token  = next((t for t in tokens if t.get("outcome", "").upper() == "NO"), None)

                    if not yes_token or not no_token:
                        continue

                    yes_price = float(yes_token.get("price", 0) or 0)
                    no_price  = float(no_token.get("price", 0) or 0)

                    if yes_price <= 0 or no_price <= 0:
                        continue

                    markets.append(PolyMarket(
                        condition_id=m["condition_id"],
                        question=m.get("question", ""),
                        yes_price=yes_price,
                        no_price=no_price,
                        volume=volume,
                        liquidity=liquidity,
                        end_date=m.get("end_date_iso"),
                        tags=tags,
                    ))
                except (KeyError, ValueError, TypeError):
                    continue

            next_cursor = data.get("next_cursor")
            if not next_cursor or next_cursor == "LTE=":
                break

        logger.info(f"Polymarket: {len(markets)} mercados elegíveis encontrados")
        return markets

# ─── CSGOEMPIRE CLIENT ────────────────────────────────────────────────────────
class CSGOEmpireClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            "Authorization": f"Bearer {CSGOEMPIRE_API_KEY}",
            "Content-Type": "application/json",
        }

    async def get_events(self) -> list[EmpireEvent]:
        """Busca eventos desportivos disponíveis para apostas no CSGOEmpire."""
        try:
            async with self.session.get(
                f"{CSGOEMPIRE_API_BASE}/sports/events",
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 401:
                    logger.error("CSGOEmpire: API key inválida ou sem permissões de sports betting")
                    return []
                if resp.status != 200:
                    logger.warning(f"CSGOEmpire API error: {resp.status} — {await resp.text()}")
                    return []
                data = await resp.json()
        except Exception as e:
            logger.error(f"CSGOEmpire fetch error: {e}")
            return []

        events = []
        for e in data.get("data", []):
            try:
                odds = e.get("odds", {})
                odds_a = float(odds.get("home", 0) or 0)
                odds_b = float(odds.get("away", 0) or 0)
                if odds_a <= 1 or odds_b <= 1:
                    continue

                events.append(EmpireEvent(
                    match_id=str(e["id"]),
                    title=e.get("name", ""),
                    team_a=e.get("home_team", "Team A"),
                    team_b=e.get("away_team", "Team B"),
                    odds_a=odds_a,
                    odds_b=odds_b,
                    draw_odds=float(odds["draw"]) if odds.get("draw") else None,
                    sport=e.get("sport", "").lower(),
                    starts_at=e.get("starts_at"),
                ))
            except (KeyError, ValueError, TypeError):
                continue

        logger.info(f"CSGOEmpire: {len(events)} eventos encontrados")
        return events

    async def place_bet(self, match_id: str, side: str, stake_usd: float) -> dict:
        """Coloca aposta no CSGOEmpire. side = 'home' | 'away' | 'draw'"""
        payload = {
            "match_id": match_id,
            "side": side,
            "amount": round(stake_usd, 2),
        }
        try:
            async with self.session.post(
                f"{CSGOEMPIRE_API_BASE}/sports/bet",
                headers=self.headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                result = await resp.json()
                if resp.status == 200 and result.get("success"):
                    logger.info(f"✅ Aposta colocada: {match_id} | {side} | ${stake_usd}")
                    return {"success": True, "data": result}
                else:
                    logger.warning(f"❌ Aposta falhou: {result}")
                    return {"success": False, "data": result}
        except Exception as e:
            logger.error(f"Erro ao colocar aposta: {e}")
            return {"success": False, "error": str(e)}

# ─── MATCHING ENGINE ──────────────────────────────────────────────────────────
def decimal_odds_to_prob(odds: float) -> float:
    """Converte odds decimais para probabilidade implícita."""
    return 1.0 / odds if odds > 0 else 0.0

def find_arbitrage(
    poly_markets: list[PolyMarket],
    empire_events: list[EmpireEvent],
) -> list[ArbOpportunity]:
    """
    Tenta fazer match entre mercados Polymarket e eventos CSGOEmpire
    usando palavras-chave nos títulos e calcula edge de arbitragem.
    """
    opportunities = []

    for poly in poly_markets:
        q = poly.question.lower()

        for ev in empire_events:
            # Match fuzzy: team names ou título do evento na pergunta do Polymarket
            team_a_low = ev.team_a.lower()
            team_b_low = ev.team_b.lower()

            matched_a = team_a_low in q or any(w in q for w in team_a_low.split() if len(w) > 3)
            matched_b = team_b_low in q or any(w in q for w in team_b_low.split() if len(w) > 3)

            if not (matched_a and matched_b):
                continue

            # Determinar qual outcome do Polymarket corresponde a qual equipa
            # Heurística: se YES menciona a equipa A, YES = vitória equipa A
            yes_is_team_a = team_a_low in q[:len(q)//2] or "win" in q

            checks = []
            # Cenário 1: Apostar YES no Poly + AWAY no Empire (se YES = team_b)
            # Cenário 2: Apostar NO no Poly + HOME no Empire (se YES = team_a, NO = team_b)

            combos = [
                # (poly_side, poly_price, empire_side, empire_odds)
                ("YES", poly.yes_price, "home" if yes_is_team_a else "away",
                 ev.odds_a if yes_is_team_a else ev.odds_b),
                ("NO",  poly.no_price,  "away" if yes_is_team_a else "home",
                 ev.odds_b if yes_is_team_a else ev.odds_a),
            ]

            for poly_side, poly_price, empire_side, empire_odds in combos:
                empire_implied = decimal_odds_to_prob(empire_odds)

                # Edge = diferença entre o que Poly paga e o que Empire implica
                # Poly: apostamos poly_price para ganhar ~1 se o evento acontecer
                # Se o evento realmente tem probabilidade empire_implied,
                # o valor esperado de apostar YES no Poly é:
                #   EV = empire_implied * (1/poly_price) - 1   (sem fees)
                # Após fees Poly (POLY_FEE) e considerando o hedge no Empire:

                # Poly payout por unidade apostada (após fee):
                poly_net_payout = (1.0 / poly_price) * (1 - POLY_FEE)

                # Empire payout por unidade apostada (após fee):
                empire_net_payout = empire_odds * (1 - EMPIRE_FEE)

                # Edge de arbitragem pura (dois lados contrários):
                # Apostamos X no Poly (YES) e Y no Empire (away) tal que
                # os payouts se igualem → edge = 1 - (poly_price + 1/empire_odds)
                arb_sum = poly_price + (1.0 / empire_odds) if empire_odds > 0 else 999

                # Edge líquido (positivo = oportunidade)
                raw_edge = (1.0 / arb_sum) - 1  # retorno sobre o total apostado
                # Descontar fees
                net_edge = raw_edge - POLY_FEE - EMPIRE_FEE

                if net_edge < MIN_ARB_EDGE:
                    continue

                # Stake ótima (Kelly simplificado, cap em MAX_AUTO_BET_USD)
                stake = min(MAX_AUTO_BET_USD, MAX_AUTO_BET_USD * net_edge * 10)
                expected_profit = stake * net_edge

                opportunities.append(ArbOpportunity(
                    poly_market=poly,
                    empire_event=ev,
                    poly_side=poly_side,
                    empire_side=empire_side,
                    poly_price=poly_price,
                    empire_implied=empire_implied,
                    edge=net_edge,
                    recommended_stake_usd=round(stake, 2),
                    expected_profit_usd=round(expected_profit, 2),
                ))

    # Ordenar por edge decrescente
    opportunities.sort(key=lambda o: o.edge, reverse=True)
    logger.info(f"🎯 {len(opportunities)} oportunidades de arbitragem encontradas")
    return opportunities

# ─── TELEGRAM NOTIFIER ───────────────────────────────────────────────────────
class TelegramNotifier:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.base = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

    async def send(self, text: str, parse_mode: str = "HTML"):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning("Telegram não configurado — a imprimir alerta no terminal:")
            print(text)
            return
        try:
            async with self.session.post(
                f"{self.base}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Telegram erro: {resp.status} — {await resp.text()}")
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

    def format_opportunity(self, opp: ArbOpportunity, auto_executed: bool = False, exec_result: dict = None) -> str:
        status_icon = "🤖 AUTO-EXECUTADO" if auto_executed else "⚠️ OPORTUNIDADE"
        exec_status = ""
        if auto_executed and exec_result:
            if exec_result.get("success"):
                exec_status = "\n✅ <b>Aposta colocada com sucesso no Empire!</b>"
            else:
                exec_status = f"\n❌ <b>Erro ao executar:</b> {exec_result.get('data', {})}"

        return (
            f"{'🔥' * 3} <b>{status_icon}</b> {'🔥' * 3}\n\n"
            f"📊 <b>Polymarket:</b>\n"
            f"  ❓ {opp.poly_market.question}\n"
            f"  🎯 Apostar <b>{opp.poly_side}</b> @ <b>{opp.poly_price:.3f}</b>\n"
            f"  💰 Volume: <b>${opp.poly_market.volume:,.0f}</b> | Liquidez: <b>${opp.poly_market.liquidity:,.0f}</b>\n\n"
            f"🎮 <b>CSGOEmpire:</b>\n"
            f"  ⚔️ {opp.empire_event.team_a} vs {opp.empire_event.team_b}\n"
            f"  🎯 Apostar <b>{opp.empire_side.upper()}</b>\n"
            f"  📈 Odds Empire: <b>{1/opp.empire_implied:.2f}</b> (impl. {opp.empire_implied:.1%})\n\n"
            f"💹 <b>Edge líquido:</b> <b>{opp.edge:.2%}</b>\n"
            f"💵 <b>Stake recomendado:</b> ${opp.recommended_stake_usd}\n"
            f"🎁 <b>Lucro esperado:</b> ${opp.expected_profit_usd}"
            f"{exec_status}\n"
            f"\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )

# ─── MAIN BOT LOOP ────────────────────────────────────────────────────────────
class ArbBot:
    def __init__(self):
        self.seen_opportunities: set[str] = set()  # evita alertas duplicados

    def _opportunity_key(self, opp: ArbOpportunity) -> str:
        return f"{opp.poly_market.condition_id}_{opp.empire_event.match_id}_{opp.poly_side}"

    async def run(self):
        logger.info("🚀 Arbitrage Bot iniciado")
        async with aiohttp.ClientSession() as session:
            poly_client   = PolymarketClient(session)
            empire_client = CSGOEmpireClient(session)
            telegram      = TelegramNotifier(session)

            mode_str = '⚠️ Modo: Só Alertas (sem API Empire)' if not bool(CSGOEMPIRE_API_KEY and CSGOEMPIRE_API_KEY.strip()) else '🤖 Modo: Auto-execução ativo'
            await telegram.send(
                "🤖 <b>Arbitrage Bot Iniciado</b>\n"
                f"{mode_str}\n"
                f"Monitorização: Desporto + Esports/CS2\n"
                f"Volume mínimo Polymarket: ${MIN_POLY_VOLUME:,}\n"
                f"Edge mínimo: {MIN_ARB_EDGE:.0%}\n"
                f"Scan a cada {SCAN_INTERVAL}s ✅"
            )

            while True:
                try:
                    logger.info("🔍 A procurar oportunidades...")

                    poly_markets  = await poly_client.get_markets()
                    empire_events = await empire_client.get_events()
                    opportunities = find_arbitrage(poly_markets, empire_events)

                    new_count = 0
                    for opp in opportunities:
                        key = self._opportunity_key(opp)
                        if key in self.seen_opportunities:
                            continue

                        self.seen_opportunities.add(key)
                        new_count += 1

                        # Executar automaticamente se API key configurada, caso contrário só alertas
                        alerts_only = not bool(CSGOEMPIRE_API_KEY and CSGOEMPIRE_API_KEY.strip())
                        if alerts_only:
                            msg = telegram.format_opportunity(opp, auto_executed=False)
                        else:
                            empire_side_map = {"home": "home", "away": "away", "draw": "draw"}
                            exec_result = await empire_client.place_bet(
                                match_id=opp.empire_event.match_id,
                                side=empire_side_map.get(opp.empire_side, opp.empire_side),
                                stake_usd=opp.recommended_stake_usd,
                            )
                            msg = telegram.format_opportunity(opp, auto_executed=True, exec_result=exec_result)
                        await telegram.send(msg)
                        await asyncio.sleep(1)  # rate limit Telegram

                    if new_count == 0 and len(opportunities) > 0:
                        logger.info(f"ℹ️ {len(opportunities)} oportunidades já vistas anteriormente")
                    elif new_count == 0:
                        logger.info("ℹ️ Nenhuma oportunidade nova encontrada")

                    # Limpar chaves vistas a cada 500 para não crescer indefinidamente
                    if len(self.seen_opportunities) > 500:
                        self.seen_opportunities.clear()

                except Exception as e:
                    logger.error(f"Erro no loop principal: {e}", exc_info=True)
                    await telegram.send(f"⚠️ Bot erro: {e}")

                await asyncio.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    asyncio.run(ArbBot().run())
