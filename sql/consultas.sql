-- 0.1 Contagem de registros por tabela
SELECT 'dim_data'     AS tabela, COUNT(*) AS total FROM dim_data
UNION ALL
SELECT 'dim_cliente',  COUNT(*) FROM dim_cliente
UNION ALL
SELECT 'dim_produto',  COUNT(*) FROM dim_produto
UNION ALL
SELECT 'dim_vendedor', COUNT(*) FROM dim_vendedor
UNION ALL
SELECT 'fato_pedido',  COUNT(*) FROM fato_pedido;


-- 1. FATURAMENTO TOTAL E TICKET MÉDIO POR MÊS
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    COUNT(DISTINCT f.order_id)          AS qtd_pedidos,
    SUM(f.valor_produto)                AS receita_produtos,
    SUM(f.valor_frete)                  AS receita_frete,
    SUM(f.valor_total)                  AS faturamento_total,
    ROUND(AVG(f.valor_produto), 2)      AS ticket_medio_produto
FROM fato_pedido f
JOIN dim_data d ON f.id_data_compra = d.id_data
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
GROUP BY d.ano, d.mes, d.nome_mes
ORDER BY d.ano, d.mes;


-- 2. TOP 10 CATEGORIAS DE PRODUTOS POR FATURAMENTO
SELECT
    p.categoria_en                              AS categoria,
    COUNT(*)                                    AS qtd_itens,
    COUNT(DISTINCT f.order_id)                  AS qtd_pedidos,
    SUM(f.valor_produto)                        AS faturamento,
    ROUND(AVG(f.valor_produto), 2)              AS ticket_medio,
    ROUND(100.0 * SUM(f.valor_produto)
        / NULLIF(SUM(SUM(f.valor_produto)) OVER (), 0), 2) AS pct_total
FROM fato_pedido f
JOIN dim_produto p ON f.id_produto = p.id_produto
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
GROUP BY p.categoria_en
ORDER BY faturamento DESC
LIMIT 10;


-- 3. DISTRIBUIÇÃO DE PEDIDOS POR STATUS
SELECT
    f.status_pedido,
    COUNT(DISTINCT f.order_id)  AS qtd_pedidos,
    ROUND(100.0 * COUNT(DISTINCT f.order_id)
        / NULLIF(SUM(COUNT(DISTINCT f.order_id)) OVER (), 0), 2) AS pct
FROM fato_pedido f
GROUP BY f.status_pedido
ORDER BY qtd_pedidos DESC;


-- 4. ANÁLISE DE AVALIAÇÕES (NPS simplificado)
SELECT
    f.nota_avaliacao,
    COUNT(*)                          AS qtd,
    ROUND(100.0 * COUNT(*)
        / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct
FROM fato_pedido f
WHERE f.nota_avaliacao IS NOT NULL
GROUP BY f.nota_avaliacao
ORDER BY f.nota_avaliacao;

-- Média geral de avaliação
SELECT ROUND(AVG(nota_avaliacao::NUMERIC), 2) AS nota_media
FROM fato_pedido
WHERE nota_avaliacao IS NOT NULL;


-- 5. PRAZO MÉDIO DE ENTREGA POR ESTADO DO CLIENTE
SELECT
    c.estado,
    COUNT(DISTINCT f.order_id)          AS pedidos_entregues,
    ROUND(AVG(f.dias_para_entrega), 1)  AS prazo_medio_dias,
    MIN(f.dias_para_entrega)            AS prazo_minimo,
    MAX(f.dias_para_entrega)            AS prazo_maximo
FROM fato_pedido f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
WHERE f.status_pedido = 'delivered'
  AND f.dias_para_entrega IS NOT NULL
  AND f.dias_para_entrega >= 0
GROUP BY c.estado
ORDER BY prazo_medio_dias DESC;


-- 6. TOP 10 ESTADOS POR FATURAMENTO
SELECT
    c.estado,
    COUNT(DISTINCT c.id_cliente)        AS qtd_clientes,
    COUNT(DISTINCT f.order_id)          AS qtd_pedidos,
    SUM(f.valor_total)                  AS faturamento_total,
    ROUND(AVG(f.valor_produto), 2)      AS ticket_medio
FROM fato_pedido f
JOIN dim_cliente c ON f.id_cliente = c.id_cliente
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
GROUP BY c.estado
ORDER BY faturamento_total DESC
LIMIT 10;


-- 7. TOP 10 VENDEDORES POR FATURAMENTO
SELECT
    v.seller_id,
    v.cidade                            AS cidade_vendedor,
    v.estado                            AS estado_vendedor,
    COUNT(DISTINCT f.order_id)          AS qtd_pedidos,
    SUM(f.valor_produto)                AS faturamento,
    ROUND(AVG(f.nota_avaliacao), 2)     AS nota_media,
    ROUND(AVG(f.dias_para_entrega), 1)  AS prazo_medio_dias
FROM fato_pedido f
JOIN dim_vendedor v ON f.id_vendedor = v.id_vendedor
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
GROUP BY v.seller_id, v.cidade, v.estado
ORDER BY faturamento DESC
LIMIT 10;


-- 8. EVOLUÇÃO DO VOLUME DE PEDIDOS POR DIA DA SEMANA
SELECT
    d.dia_semana_num,
    d.dia_semana_nome,
    COUNT(DISTINCT f.order_id)          AS qtd_pedidos,
    ROUND(AVG(f.valor_produto), 2)      AS ticket_medio
FROM fato_pedido f
JOIN dim_data d ON f.id_data_compra = d.id_data
GROUP BY d.dia_semana_num, d.dia_semana_nome
ORDER BY d.dia_semana_num;


-- 9. RELAÇÃO ENTRE NOTA DE AVALIAÇÃO E PRAZO DE ENTREGA
SELECT
    f.nota_avaliacao,
    COUNT(*)                            AS qtd_pedidos,
    ROUND(AVG(f.dias_para_entrega), 1)  AS prazo_medio_dias,
    ROUND(AVG(f.valor_produto), 2)      AS ticket_medio
FROM fato_pedido f
WHERE f.nota_avaliacao IS NOT NULL
  AND f.dias_para_entrega IS NOT NULL
  AND f.status_pedido = 'delivered'
GROUP BY f.nota_avaliacao
ORDER BY f.nota_avaliacao;


-- 10. SAZONALIDADE — FATURAMENTO POR TRIMESTRE
SELECT
    d.ano,
    d.trimestre,
    COUNT(DISTINCT f.order_id)          AS qtd_pedidos,
    SUM(f.valor_total)                  AS faturamento_total,
    ROUND(AVG(f.valor_produto), 2)      AS ticket_medio
FROM fato_pedido f
JOIN dim_data d ON f.id_data_compra = d.id_data
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
GROUP BY d.ano, d.trimestre
ORDER BY d.ano, d.trimestre;


-- 11. ANÁLISE DE FORMAS DE PAGAMENTO
SELECT
    f.tipo_pagamento,
    COUNT(DISTINCT f.order_id)          AS qtd_pedidos,
    SUM(f.valor_pago)                   AS valor_total_pago,
    ROUND(AVG(f.valor_pago), 2)         AS ticket_medio,
    ROUND(100.0 * COUNT(DISTINCT f.order_id)
        / NULLIF(SUM(COUNT(DISTINCT f.order_id)) OVER (), 0), 2) AS pct_pedidos
FROM fato_pedido f
WHERE f.tipo_pagamento IS NOT NULL
  AND f.tipo_pagamento != 'NaN'
GROUP BY f.tipo_pagamento
ORDER BY qtd_pedidos DESC;


-- 12. DISTRIBUIÇÃO DE PARCELAMENTO (CARTÃO DE CRÉDITO)
SELECT
    f.parcelas,
    COUNT(*)                            AS qtd_itens,
    ROUND(AVG(f.valor_pago), 2)         AS ticket_medio
FROM fato_pedido f
WHERE f.tipo_pagamento = 'credit_card'
  AND f.parcelas IS NOT NULL
GROUP BY f.parcelas
ORDER BY f.parcelas;