# Documentação do Dashboard — Olist E-Commerce

## Visão Geral

O dashboard foi desenvolvido no Power BI Desktop e conecta diretamente ao banco PostgreSQL `dw_olist` via DirectQuery ou Import Mode. Está organizado em **3 páginas**, cada uma com foco em um ângulo estratégico diferente do negócio.

---

## Página 1 — Visão Geral

**Objetivo:** Apresentar os principais KPIs do negócio em uma única tela de alto nível.

![Página 1](imagens/Pg1Olist.png)

### Cartões de KPI (topo)
| Métrica              | Descrição                                              |
|----------------------|--------------------------------------------------------|
| Total de Pedidos     | COUNT DISTINCT de order_id na fato                     |
| Faturamento Total    | SUM de valor_total (excluindo cancelados)              |
| Ticket Médio         | Média de valor_produto por pedido                      |
| Nota Média           | Média de nota_avaliacao (1 a 5)                        |
| Taxa de Entrega      | % de pedidos com status = 'delivered'                  |

### Gráficos
- **Pedidos por status** (gráfico de rosca) — distributed, canceled, shipped, etc.
- **Faturamento por trimestre** (gráfico de barras agrupadas por ano)

### Filtros (segmentações)
- Data
- Estado do cliente

---

## Página 2 — Produtos e Categorias

**Objetivo:** Analisar o desempenho por categoria de produto e identificar os mais relevantes.

![Página 2](imagens/Pg2Olist.png)

### Gráficos
- **Top 10 categorias por faturamento** (gráfico de barras horizontais)
  - Eixo X: faturamento total
  - Eixo Y: categoria_pt da dim_produto
- **Participação das categorias no faturamento total** (gráfico de rosca — top 5 + outros)
- **Ticket médio por categoria** (gráfico de barras verticais)


### Filtros
- Estado do cliente
- Data

---

## Página 3 — Clientes, Vendedores e Logística

**Objetivo:** Entender a distribuição geográfica dos clientes e o desempenho de entrega por estado.

![Página 3](imagens/Pg3Olist.png)

### Gráficos
- **Faturamento por estado (UF do cliente)** (mapa do Brasil ou gráfico de barras horizontais)
- **Prazo médio de entrega por estado** (gráfico de barras ordenado por dias)
- **Pedidos por dia da semana** (gráfico de barras — Segunda a Domingo)
- **Top 10 vendedores por faturamento** (tabela com seller_id, cidade, estado, faturamento e nota média)

### Filtros
- Estado do cliente
- Data

---

## Perguntas de Negócio Respondidas pelo Dashboard

1. Qual é o faturamento por trimestre e sua evolução ao longo dos anos?
2. Quais categorias de produto geram mais receita?
3. Qual é a nota média de satisfação dos clientes?
4. Qual é o ticket médio por categoria?
5. Qual é a proporção de pedidos por status?
6. Qual é a taxa de entrega dos pedidos?
7. Quais estados concentram mais pedidos e faturamento?
8. Quais estados têm o pior prazo de entrega?
9. Quais dias da semana concentram mais compras?
10. Quais são os vendedores com melhor desempenho?
---

## Conexão com o Banco de Dados

1. Abra o Power BI Desktop
2. Clique em **Obter Dados → PostgreSQL**
3. Servidor: `localhost` | Banco: `dw_olist`
4. Carregue as tabelas: `fato_pedido`, `dim_data`, `dim_cliente`, `dim_produto`, `dim_vendedor`
5. O Power BI detecta as FKs automaticamente — confirme os relacionamentos no modelo

### Relacionamentos esperados
| De                          | Para                    | Cardinalidade |
|-----------------------------|-------------------------|---------------|
| fato_pedido.id_data_compra  | dim_data.id_data        | N:1           |
| fato_pedido.id_data_entrega | dim_data.id_data        | N:1           |
| fato_pedido.id_cliente      | dim_cliente.id_cliente  | N:1           |
| fato_pedido.id_produto      | dim_produto.id_produto  | N:1           |
| fato_pedido.id_vendedor     | dim_vendedor.id_vendedor| N:1           |

