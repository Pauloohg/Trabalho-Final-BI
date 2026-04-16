import os
import argparse
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# DDL


DDL = """
CREATE TABLE IF NOT EXISTS dim_data (
    id_data         INTEGER     PRIMARY KEY,
    data            DATE        NOT NULL,
    dia             SMALLINT    NOT NULL,
    mes             SMALLINT    NOT NULL,
    nome_mes        VARCHAR(15) NOT NULL,
    trimestre       SMALLINT    NOT NULL,
    ano             SMALLINT    NOT NULL,
    dia_semana_num  SMALLINT    NOT NULL,
    dia_semana_nome VARCHAR(15) NOT NULL,
    fim_de_semana   BOOLEAN     NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_cliente (
    id_cliente         SERIAL       PRIMARY KEY,
    customer_unique_id VARCHAR(50)  NOT NULL UNIQUE,
    cidade             VARCHAR(100),
    estado             CHAR(2)
);

CREATE TABLE IF NOT EXISTS dim_produto (
    id_produto   SERIAL       PRIMARY KEY,
    product_id   VARCHAR(50)  NOT NULL UNIQUE,
    categoria    VARCHAR(100),
    categoria_en VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_vendedor (
    id_vendedor SERIAL      PRIMARY KEY,
    seller_id   VARCHAR(50) NOT NULL UNIQUE,
    cidade      VARCHAR(100),
    estado      CHAR(2)
);

CREATE TABLE IF NOT EXISTS fato_pedido (
    id_fato            SERIAL         PRIMARY KEY,
    order_id           VARCHAR(50)    NOT NULL,
    id_data_compra     INTEGER        NOT NULL REFERENCES dim_data(id_data),
    id_data_entrega    INTEGER        REFERENCES dim_data(id_data),
    id_cliente         INTEGER        NOT NULL REFERENCES dim_cliente(id_cliente),
    id_produto         INTEGER        NOT NULL REFERENCES dim_produto(id_produto),
    id_vendedor        INTEGER        NOT NULL REFERENCES dim_vendedor(id_vendedor),
    status_pedido      VARCHAR(30),
    valor_produto      NUMERIC(12,2)  NOT NULL,
    valor_frete        NUMERIC(12,2),
    valor_total        NUMERIC(12,2),
    tipo_pagamento     VARCHAR(30),
    parcelas           SMALLINT,
    valor_pago         NUMERIC(12,2),
    nota_avaliacao     SMALLINT,
    qtd_itens          SMALLINT       NOT NULL DEFAULT 1,
    dias_para_entrega  SMALLINT
);
"""

NOMES_MESES = {
    1: "Janeiro",   2: "Fevereiro", 3: "Março",    4: "Abril",
    5: "Maio",      6: "Junho",     7: "Julho",     8: "Agosto",
    9: "Setembro", 10: "Outubro",  11: "Novembro", 12: "Dezembro",
}
NOMES_DIAS = {
    0: "Segunda-feira", 1: "Terça-feira",  2: "Quarta-feira",
    3: "Quinta-feira",  4: "Sexta-feira",  5: "Sábado", 6: "Domingo",
}

# Categorias que não estavam na tabela
TRADUCAO_MANUAL = {
    "pc_gamer":                                       "PC Gamer",
    "portateis_cozinha_e_preparadores_de_alimentos":  "Portable Kitchen & Food Processors",
    "":                                               "Uncategorized",
}


# EXTRACT
def extrair_csvs(pasta):
    arquivos = {
        "pedidos":    "olist_orders_dataset.csv",
        "itens":      "olist_order_items_dataset.csv",
        "clientes":   "olist_customers_dataset.csv",
        "produtos":   "olist_products_dataset.csv",
        "vendedores": "olist_sellers_dataset.csv",
        "avaliacoes": "olist_order_reviews_dataset.csv",
        "pagamentos": "olist_order_payments_dataset.csv",
        "traducao":   "product_category_name_translation.csv",
    }
    dfs = {}
    for chave, nome in arquivos.items():
        caminho = os.path.join(pasta, nome)
        if not os.path.exists(caminho):
            raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")

        if chave == "avaliacoes":
            dfs[chave] = pd.read_csv(
                caminho,
                dtype=str,
                keep_default_na=False,
                usecols=["order_id", "review_score", "review_answer_timestamp"],
            )
        else:
            dfs[chave] = pd.read_csv(caminho, dtype=str, keep_default_na=False)

        print(f"  Lido: {nome} ({len(dfs[chave])} linhas)")
    return dfs


# HELPERS
def _parse_data(texto):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(texto).strip(), fmt).date()
        except Exception:
            pass
    return None


def _parse_decimal(valor):
    try:
        v = str(valor).strip()
        if v in ("", "-", "nan"):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def _dias_entre(d1, d2):
    if d1 is None or d2 is None:
        return None
    try:
        delta = (d2 - d1).days
        return int(delta) if delta >= 0 else None
    except Exception:
        return None


def _none(val):
    import math
    if val is None:
        return None
    if isinstance(val, str) and val.strip().lower() in ("nan", "none", ""):
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else val
    except (TypeError, ValueError):
        return val if str(val).strip() != "" else None
    

# TRANSFORM
def transformar(dfs):
    pedidos    = dfs["pedidos"].copy()
    itens      = dfs["itens"].copy()
    clientes   = dfs["clientes"].copy()
    produtos   = dfs["produtos"].copy()
    vendedores = dfs["vendedores"].copy()
    avaliacoes = dfs["avaliacoes"].copy()
    pagamentos = dfs["pagamentos"].copy()
    traducao   = dfs["traducao"].copy()
    # geolocation removido: estado já presente em dim_cliente e dim_vendedor

    mapa_customer_id = clientes.set_index("customer_id")["customer_unique_id"].to_dict()

    clientes = (
        clientes[["customer_id", "customer_unique_id", "customer_city", "customer_state"]]
        .rename(columns={"customer_city": "cidade", "customer_state": "estado"})
        .drop_duplicates(subset=["customer_unique_id"])
    )

    traducao.columns = [c.lstrip("\ufeff").strip() for c in traducao.columns]
    traducao = traducao.rename(columns={
        "product_category_name":         "categoria",
        "product_category_name_english": "categoria_en",
    })

    produtos = produtos[["product_id", "product_category_name"]].rename(
        columns={"product_category_name": "categoria"}
    )
    produtos["categoria"] = produtos["categoria"].str.strip()

    produtos = produtos.merge(traducao, on="categoria", how="left")

    # Preenche as 3 categorias sem tradução via dicionário manual
    def _traduzir(row):
        if pd.isna(row["categoria_en"]) or str(row["categoria_en"]).strip() == "":
            return TRADUCAO_MANUAL.get(row["categoria"], row["categoria"])
        return row["categoria_en"]

    produtos["categoria_en"] = produtos.apply(_traduzir, axis=1)
    produtos["categoria"] = produtos["categoria"].replace("", "uncategorized")
    produtos = produtos.drop_duplicates(subset=["product_id"])

    vendedores = vendedores[["seller_id", "seller_city", "seller_state"]].rename(
        columns={"seller_city": "cidade", "seller_state": "estado"}
    )

    avaliacoes["review_answer_timestamp"] = avaliacoes["review_answer_timestamp"].apply(_parse_data)
    avaliacoes = (
        avaliacoes
        .sort_values("review_answer_timestamp", ascending=False, na_position="last")
        .drop_duplicates(subset=["order_id"])
        [["order_id", "review_score"]]
    )
    avaliacoes["review_score"] = pd.to_numeric(avaliacoes["review_score"], errors="coerce")

    pagamentos["payment_value"]        = pd.to_numeric(pagamentos["payment_value"],        errors="coerce").fillna(0)
    pagamentos["payment_installments"] = pd.to_numeric(pagamentos["payment_installments"], errors="coerce").fillna(0)
    pagamentos["payment_installments"] = pagamentos["payment_installments"].apply(
        lambda x: 1 if x == 0 else x
    )

    def _agregar_pagamento(grupo):
        idx_principal = grupo["payment_value"].idxmax()
        return pd.Series({
            "tipo_pagamento": grupo.loc[idx_principal, "payment_type"],
            "parcelas":       int(grupo["payment_installments"].max()),
            "valor_pago":     round(grupo["payment_value"].sum(), 2),
        })

    pagamentos_agg = pagamentos.groupby("order_id").apply(_agregar_pagamento).reset_index()

    pedidos["data_compra"]  = pedidos["order_purchase_timestamp"].apply(_parse_data)
    pedidos["data_entrega"] = pedidos["order_delivered_customer_date"].apply(_parse_data)

    antes = len(pedidos)
    pedidos = pedidos.dropna(subset=["data_compra"])
    descartados = antes - len(pedidos)
    if descartados:
        print(f"  Aviso: {descartados} pedido(s) sem data de compra removidos")

    itens["valor_produto"] = itens["price"].apply(_parse_decimal)
    itens["valor_frete"]   = itens["freight_value"].apply(_parse_decimal)
    itens["valor_total"]   = itens.apply(
        lambda r: (r["valor_produto"] or 0.0) + (r["valor_frete"] or 0.0), axis=1
    )
    qtd_itens = itens.groupby("order_id").size().reset_index(name="qtd_itens")

    df = itens.merge(
        pedidos[["order_id", "customer_id", "order_status", "data_compra", "data_entrega"]],
        on="order_id", how="inner"
    )

    df["customer_unique_id"] = df["customer_id"].map(mapa_customer_id)
    df = df.merge(avaliacoes,     on="order_id", how="left")
    df = df.merge(pagamentos_agg, on="order_id", how="left")
    df = df.merge(qtd_itens,      on="order_id", how="left")

    df["qtd_itens"]        = df["qtd_itens"].fillna(1).astype(int)
    df["dias_para_entrega"] = df.apply(
        lambda r: _dias_entre(r["data_compra"], r["data_entrega"]), axis=1
    )

    return df, clientes, produtos, vendedores