# 🤖 Arbitrage Bot — Polymarket ↔ CSGOEmpire

Bot que monitoriza oportunidades de arbitragem entre **Polymarket** e **CSGOEmpire**,
envia alertas via **Telegram** e executa apostas automaticamente no Empire.

---

## ⚙️ Requisitos

- Python 3.11+
- Conta no CSGOEmpire com sports betting ativo
- Bot Telegram criado via @BotFather

---

## 🚀 Instalação

```bash
# 1. Clonar / copiar os ficheiros para uma pasta
cd arb-bot

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar variáveis de ambiente
cp .env.example .env
# Edita o .env com os teus tokens/API keys

# 4. Iniciar o bot
python bot.py
```

---

## 🔑 Configuração do .env

| Variável | Descrição | Onde obter |
|---|---|---|
| `TELEGRAM_TOKEN` | Token do bot Telegram | @BotFather → /newbot |
| `TELEGRAM_CHAT_ID` | O teu chat ID | @userinfobot |
| `CSGOEMPIRE_API_KEY` | API Key do Empire | csgoempire.com → Perfil → API Keys |
| `MAX_AUTO_BET_USD` | Stake máximo por aposta (default: 50) | Define tu |

---

## 📐 Como funciona a arbitragem

```
Polymarket (YES @ 0.40) + CSGOEmpire (AWAY odds 2.80)

Probabilidade implícita Empire: 1/2.80 = 35.7%
Polymarket cobra 40¢ para pagar $1 se YES

Edge bruto = 1 - (0.40 + 1/2.80) = 1 - 0.757 = 24.3%
Edge após fees (2% Poly + 5% Empire) = ~17.3%

→ Apostas opostas nos dois lados garantem lucro independentemente do resultado
```

### Filtros ativos
- ✅ Volume Polymarket ≥ $10,000
- ✅ Liquidez Polymarket ≥ $10,000  
- ✅ Edge mínimo: 4% após fees
- ✅ Mercados: Desporto + Esports/CS2

---

## 📱 Exemplo de alerta Telegram

```
🔥🔥🔥 🤖 AUTO-EXECUTADO 🔥🔥🔥

📊 Polymarket:
  ❓ Will Vitality win vs NAVI in ESL Pro League?
  🎯 Apostar YES @ 0.420
  💰 Volume: $45,230 | Liquidez: $12,100

🎮 CSGOEmpire:
  ⚔️ Vitality vs NAVI
  🎯 Apostar HOME
  📈 Odds Empire: 2.60 (impl. 38.5%)

💹 Edge líquido: 8.50%
💵 Stake recomendado: $42.50
🎁 Lucro esperado: $3.61

✅ Aposta colocada com sucesso no Empire!

🕐 2026-03-12 14:32 UTC
```

---

## ⚠️ Avisos Importantes

### API do CSGOEmpire
O endpoint `/api/v2/sports/events` e `/api/v2/sports/bet` **podem não estar
disponíveis publicamente** ou exigir permissões especiais. Se receberes erros 401/404:

1. Contacta o suporte do CSGOEmpire para acesso à API de sports betting
2. Verifica a documentação oficial em https://csgoempire.com/api/documentation
3. Confirma que a tua API key tem permissões `sports_betting`

### Ajustar fees
Edita em `bot.py`:
```python
POLY_FEE   = 0.02   # 2% Polymarket
EMPIRE_FEE = 0.05   # 5% CSGOEmpire (ajusta conforme o teu tier)
```

### Matching de mercados
O bot usa **fuzzy matching** por nomes de equipas. Para mercados muito específicos
(ex: handicaps, totais), o matching pode falhar. Monitoriza os logs `arb_bot.log`
para depurar.

---

## 🔄 Manter o bot a correr 24/7

### Com systemd (Linux/VPS):
```bash
# Criar serviço
sudo nano /etc/systemd/system/arbbot.service

[Unit]
Description=Arbitrage Bot
After=network.target

[Service]
ExecStart=/usr/bin/python3 /caminho/para/arb-bot/bot.py
WorkingDirectory=/caminho/para/arb-bot
Restart=always
User=tu

[Install]
WantedBy=multi-user.target

sudo systemctl enable arbbot
sudo systemctl start arbbot
```

### Com screen (simples):
```bash
screen -S arbbot
python bot.py
# Ctrl+A, D para desanexar
```

---

## 📊 Logs

O bot guarda logs em `arb_bot.log` com todos os scans, matches e apostas.
