# PROJETO DATA WAREHOUSE E BI

**Olist E-Commerce — Análise de Pedidos e Desempenho**

Fase 1 — Modelagem do Data Warehouse e Dicionário de Dados

Curso: Análise e Desenvolvimento de Sistemas

---

## 1. Modelo Dimensional — Star Schema

O modelo adota a arquitetura Star Schema com uma tabela fato central (FATO_PEDIDO) e quatro dimensões. Cada linha representa um item de pedido do dataset Olist, carregado individualmente na tabela fato.

### 1.1 Tabelas do Modelo

| **Tabela**    | **Tipo**  | **Descrição**                                                             |
|---------------|-----------|---------------------------------------------------------------------------|
| FATO_PEDIDO   | Fato      | Um registro por item de pedido (1 item = 1 linha fato)                    |
| DIM_DATA      | Dimensão  | Calendário com atributos temporais derivados da data de compra e entrega  |
| DIM_CLIENTE   | Dimensão  | Clientes únicos identificados pelo `customer_unique_id`                   |
| DIM_PRODUTO   | Dimensão  | Produtos com categoria em inglês e português                              |
| DIM_VENDEDOR  | Dimensão  | Vendedores com cidade e estado                                            |

### 1.2 Diagrama (Star Schema)

```
                  DIM_DATA
                 (id_data) ◄──── id_data_compra
                                 id_data_entrega
                                       │
DIM_CLIENTE ──────────────────── FATO_PEDIDO ──────────────── DIM_PRODUTO
(id_cliente)                    (id_fato PK)                  (id_produto)
                                       │
                              DIM_VENDEDOR
                              (id_vendedor)
```

---

## 2. Dicionário de Dados

### 2.1 FATO_PEDIDO

| **Coluna**         | **Tipo**       | **Nulo?** | **Descrição**                                                     |
|--------------------|----------------|-----------|-------------------------------------------------------------------|
| id_fato            | SERIAL         | NÃO       | Chave primária surrogate, autoincremento                          |
| order_id           | VARCHAR(50)    | NÃO       | Identificador do pedido (natural key do Olist)                    |
| id_data_compra     | INTEGER        | NÃO       | FK → DIM_DATA. Data em que o pedido foi realizado                 |
| id_data_entrega    | INTEGER        | SIM       | FK → DIM_DATA. Data em que o pedido foi entregue ao cliente       |
| id_cliente         | INTEGER        | NÃO       | FK → DIM_CLIENTE                                                  |
| id_produto         | INTEGER        | NÃO       | FK → DIM_PRODUTO                                                  |
| id_vendedor        | INTEGER        | NÃO       | FK → DIM_VENDEDOR                                                 |
| status_pedido      | VARCHAR(30)    | SIM       | Status do pedido: delivered, shipped, canceled, etc.              |
| valor_produto      | NUMERIC(12,2)  | NÃO       | Preço do produto no pedido (R$)                                   |
| valor_frete        | NUMERIC(12,2)  | SIM       | Valor do frete cobrado (R$)                                       |
| valor_total        | NUMERIC(12,2)  | SIM       | Soma de valor_produto + valor_frete                               |
| tipo_pagamento     | VARCHAR(30)    | SIM       | Forma principal de pagamento: credit_card, boleto, voucher, debit_card |
| parcelas           | SMALLINT       | SIM       | Número de parcelas. NULL se sem pagamento                         |
| valor_pago         | NUMERIC(12,2)  | SIM       | Valor total pago (soma de todas as formas de pagamento do pedido) |
| nota_avaliacao     | SMALLINT       | SIM       | Nota do cliente de 1 a 5. NULL se não avaliou                     |
| qtd_itens          | SMALLINT       | NÃO       | Total de itens no pedido                                          |
| dias_para_entrega  | SMALLINT       | SIM       | Diferença em dias entre data_compra e data_entrega                |

### 2.2 DIM_DATA

| **Coluna**         | **Tipo**      | **Nulo?** | **Descrição**                                              |
|--------------------|---------------|-----------|------------------------------------------------------------|
| id_data            | INTEGER       | NÃO       | PK. Chave no formato AAAAMMDD (ex.: 20171002)              |
| data               | DATE          | NÃO       | Data completa no formato ISO (AAAA-MM-DD)                  |
| dia                | SMALLINT      | NÃO       | Dia do mês (1 a 31)                                        |
| mes                | SMALLINT      | NÃO       | Mês (1 a 12)                                               |
| nome_mes           | VARCHAR(15)   | NÃO       | Nome do mês em português (ex.: Outubro)                    |
| trimestre          | SMALLINT      | NÃO       | Trimestre do ano (1 a 4)                                   |
| ano                | SMALLINT      | NÃO       | Ano (ex.: 2017, 2018)                                      |
| dia_semana_num     | SMALLINT      | NÃO       | Dia da semana numérico (0 = Segunda, 6 = Domingo)          |
| dia_semana_nome    | VARCHAR(15)   | NÃO       | Nome do dia em português (ex.: Segunda-feira)              |
| fim_de_semana      | BOOLEAN       | NÃO       | TRUE se o dia for Sábado ou Domingo                        |

### 2.3 DIM_CLIENTE

| **Coluna**          | **Tipo**      | **Nulo?** | **Descrição**                                                        |
|---------------------|---------------|-----------|----------------------------------------------------------------------|
| id_cliente          | SERIAL        | NÃO       | PK surrogate, autoincremento                                         |
| customer_unique_id  | VARCHAR(50)   | NÃO       | ID único do cliente no Olist (chave natural). UNIQUE                 |
| cidade              | VARCHAR(100)  | SIM       | Cidade do cliente                                                    |
| estado              | CHAR(2)       | SIM       | UF do cliente (ex.: SP, RJ)                                          |

### 2.4 DIM_PRODUTO

| **Coluna**    | **Tipo**      | **Nulo?** | **Descrição**                                                           |
|---------------|---------------|-----------|-------------------------------------------------------------------------|
| id_produto    | SERIAL        | NÃO       | PK surrogate, autoincremento                                            |
| product_id    | VARCHAR(50)   | NÃO       | ID do produto no Olist (chave natural). UNIQUE                          |
| categoria     | VARCHAR(100)  | SIM       | Categoria original em português (nome técnico do Olist)                 |
| categoria_pt  | VARCHAR(100)  | SIM       | Categoria traduzida para português legível (via tabela de tradução)     |

### 2.5 DIM_VENDEDOR

| **Coluna**   | **Tipo**      | **Nulo?** | **Descrição**                              |
|--------------|---------------|-----------|--------------------------------------------|
| id_vendedor  | SERIAL        | NÃO       | PK surrogate, autoincremento               |
| seller_id    | VARCHAR(50)   | NÃO       | ID do vendedor no Olist. UNIQUE            |
| cidade       | VARCHAR(100)  | SIM       | Cidade do vendedor                         |
| estado       | CHAR(2)       | SIM       | UF do vendedor                             |

---

## 3. Arquivos do Dataset (Kaggle)

Dataset: **Brazilian E-Commerce Public Dataset by Olist**
Link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Arquivos utilizados no ETL:

| **Arquivo CSV**                          | **Uso no ETL**            |
|------------------------------------------|---------------------------|
| olist_orders_dataset.csv                 | Pedidos, datas e status   |
| olist_order_items_dataset.csv            | Itens, valores e frete    |
| olist_customers_dataset.csv              | Dados dos clientes        |
| olist_products_dataset.csv               | Dados dos produtos        |
| olist_sellers_dataset.csv                | Dados dos vendedores      |
| olist_order_reviews_dataset.csv          | Notas de avaliação        |
| olist_order_payments_dataset.csv         | Tipo, parcelas e valor pago |
| product_category_name_translation.csv    | Tradução das categorias   |

---

## 4. Decisões de Modelagem

**Granularidade da fato:** um registro por item de pedido. Um pedido com 3 produtos diferentes gera 3 linhas na fato, cada uma com seu produto e vendedor correspondente.

**DIM_DATA dupla:** a tabela fato referencia `dim_data` duas vezes — uma para a data de compra (`id_data_compra`) e outra para a data de entrega (`id_data_entrega`). Isso permite análises temporais tanto pelo momento da compra quanto pelo momento da entrega.

**Cliente único:** o Olist distingue `customer_id` (por pedido) de `customer_unique_id` (por pessoa). A dimensão usa `customer_unique_id` para evitar duplicação de clientes que fizeram mais de um pedido.

**Avaliação:** pedidos com múltiplas avaliações (raro no dataset) mantêm apenas a mais recente, pré-processada no ETL.

**Pagamentos:** 2.961 pedidos usaram mais de uma forma de pagamento (ex.: cartão + voucher). O ETL agrega em uma linha por pedido: `tipo_pagamento` recebe a forma de maior valor, `parcelas` recebe o máximo e `valor_pago` recebe a soma total. Dois registros com `payment_installments = 0` são corrigidos para 1.

**Geolocation:** não utilizado no DW. O estado já está presente diretamente nas dimensões de cliente e vendedor, tornando o arquivo redundante. Pode ser excluído da pasta de dados.
