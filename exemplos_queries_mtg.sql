-- ===============================================
-- Exemplos de consultas no Data Warehouse MTG
-- ===============================================

-- 1️⃣ Consultar algumas cartas
SELECT *
FROM dim_carta
LIMIT 10;

-- 2️⃣ Consultar últimos dias cadastrados
SELECT *
FROM dim_tempo
ORDER BY date DESC
LIMIT 10;

-- 3️⃣ Consultar câmbio mais recente
SELECT *
FROM dim_cambio
ORDER BY data_id DESC
LIMIT 10;

-- 4️⃣ Consultar fatos das cartas com detalhes
SELECT f.carta_id,
       c.name_x AS nome_carta,
       f.data_id,
       t.date,
       f.preco_usd,
       f.preco_brl,
       f.power,
       f.toughness,
       f.power_score
FROM fato_cartas f
JOIN dim_carta c ON f.carta_id = c.carta_id
JOIN dim_tempo t ON f.data_id = t.data_id
ORDER BY t.date DESC
LIMIT 20;

-- 5️⃣ Contagem de registros em cada tabela
SELECT 'dim_carta' AS tabela, COUNT(*) FROM dim_carta
UNION ALL
SELECT 'dim_tempo', COUNT(*) FROM dim_tempo
UNION ALL
SELECT 'dim_cambio', COUNT(*) FROM dim_cambio
UNION ALL
SELECT 'fato_cartas', COUNT(*) FROM fato_cartas;

-- 6️⃣ Cards por raridade
SELECT c.rarity, COUNT(*) AS total_cards
FROM dim_carta c
JOIN fato_cartas f ON f.carta_id = c.carta_id
GROUP BY c.rarity
ORDER BY total_cards DESC;

-- 7️⃣ Preço médio em BRL por raridade
SELECT c.rarity, ROUND(AVG(f.preco_brl), 2) AS preco_medio_brl
FROM dim_carta c
JOIN fato_cartas f ON f.carta_id = c.carta_id
GROUP BY c.rarity
ORDER BY preco_medio_brl DESC;

-- 8️⃣ Top 5 cartas mais caras em BRL
SELECT c.name_x AS carta, f.preco_brl
FROM fato_cartas f
JOIN dim_carta c ON f.carta_id = c.carta_id
ORDER BY f.preco_brl DESC
LIMIT 5;

-- 9️⃣ Evolução de preço de uma carta específica (ex: "Black Lotus")
SELECT t.date, f.preco_usd, f.preco_brl
FROM fato_cartas f
JOIN dim_carta c ON f.carta_id = c.carta_id
JOIN dim_tempo t ON f.data_id = t.data_id
WHERE c.name_x ILIKE '%Black Lotus%'
ORDER BY t.date ASC;

-- 10️⃣ Distribuição de power_score das cartas
SELECT power_score, COUNT(*) AS total
FROM fato_cartas
GROUP BY power_score
ORDER BY total DESC;
