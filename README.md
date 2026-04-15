# DW & BI — Olist E-Commerce

Projeto de Data Warehouse e Business Intelligence sobre pedidos do marketplace Olist.

**Alunos:** Kauã Lorenseti de Oliveira e Paulo Henrique Granella  
**Curso:** Análise e Desenvolvimento de Sistemas  
**Dataset:** Brazilian E-Commerce Public Dataset by Olist (Kaggle)  
**Período dos dados:** Setembro/2016 a Outubro/2018

---

## Estrutura do Repositório

```
olist_dw/
├── dados/                          # CSVs do Kaggle (não versionado)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   └── product_category_name_translation.csv
├── docs/
│   ├── Dicionário_Dados.md          # Modelagem do DW e dicionário de dados
│   └── Doc_dashboard.md            # Documentação do dashboard Power BI
├── sql/
│   └── consultas.sql               # Consultas SQL das perguntas de negócio
├── etl.py                          # Pipeline ETL (Extract → Transform → Load)
├── requirements.txt                # Bibliotecas Python necessárias
├── .env                            # Credenciais do banco (não versionado)
├── .gitignore
└── README.md
```

---

## Pré-requisitos

- Python 3.10+
- PostgreSQL 14+
- Power BI Desktop

---

## Como Executar

### 1. Baixar o dataset

Acesse o link abaixo e faça o download de todos os arquivos CSV:  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Coloque os arquivos dentro da pasta `dados/`.

### 2. Criar o banco de dados no PostgreSQL

No pgAdmin ou via terminal:
```sql
CREATE DATABASE dw_olist;
```

### 3. Instalar as dependências Python

```bash
pip install -r requirements.txt
```

### 4. Configurar as credenciais

Edite o arquivo `.env` com os dados da sua instalação:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dw_olist
DB_USER=postgres
DB_PASSWORD=SUA_SENHA
DB_PASTA=./dados
```

### 5. Rodar o ETL

```bash
python etl.py
```

O ETL irá:
1. Ler os 7 arquivos CSV da pasta `dados/`
2. Aplicar transformações (join entre tabelas, parse de datas, tradução de categorias)
3. Criar o schema no banco (se não existir)
4. Fazer **full reload** (trunca e recarrega tudo)
5. Carregar dimensões → depois a fato

### 6. Validar os dados

Abra o arquivo `sql/consultas.sql` no pgAdmin e execute a consulta 0 para conferir a contagem de registros por tabela.

### 7. Abrir o dashboard

Conecte o Power BI Desktop ao banco `dw_olist` em `localhost` e importe as tabelas. Veja instruções detalhadas em `docs/Doc_dashboard.md`.

---

## Modelo de Dados (Star Schema)

```
              DIM_DATA
             (id_data) ◄── id_data_compra
                            id_data_entrega
                                  │
DIM_CLIENTE ──────────────── FATO_PEDIDO ──────────── DIM_PRODUTO
(id_cliente)                 (id_fato PK)             (id_produto)
                                  │
                           DIM_VENDEDOR
                           (id_vendedor)
```

| Tabela        | Descrição                                                   |
|---------------|-------------------------------------------------------------|
| `fato_pedido` | Um registro por item de pedido                              |
| `dim_data`    | Calendário com atributos temporais (usada 2× na fato)       |
| `dim_cliente` | Clientes únicos pelo `customer_unique_id`                   |
| `dim_produto` | Produtos com categoria em inglês e português                |
| `dim_vendedor`| Vendedores com cidade e estado                              |

---

## Perguntas de Negócio Respondidas

1. Faturamento total e ticket médio por mês
2. Top 10 categorias de produtos por faturamento
3. Distribuição de pedidos por status
4. Análise de avaliações dos clientes (nota 1 a 5)
5. Prazo médio de entrega por estado do cliente
6. Top 10 estados por faturamento
7. Top 10 vendedores por faturamento
8. Volume de pedidos por dia da semana
9. Relação entre nota de avaliação e prazo de entrega
10. Sazonalidade por trimestre

---

## Tecnologias Utilizadas

| Ferramenta       | Aplicação                                         |
|------------------|---------------------------------------------------|
| Python 3         | Desenvolvimento do pipeline ETL                   |
| pandas           | Leitura, transformação e junção dos CSVs          |
| psycopg2         | Conexão e carga no PostgreSQL                     |
| python-dotenv    | Gerenciamento de credenciais via .env             |
| PostgreSQL       | Banco de dados do Data Warehouse                  |
| Power BI Desktop | Criação e visualização do dashboard estratégico   |
