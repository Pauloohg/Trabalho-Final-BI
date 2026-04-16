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

# BUILD DIM_DATA
def construir_dim_data(datas):
    registros = {}
    for d in datas:
        if d is None:
            continue
        id_data = int(d.strftime("%Y%m%d"))
        if id_data not in registros:
            dow = d.weekday()
            registros[id_data] = {
                "id_data":         id_data,
                "data":            d,
                "dia":             d.day,
                "mes":             d.month,
                "nome_mes":        NOMES_MESES[d.month],
                "trimestre":       (d.month - 1) // 3 + 1,
                "ano":             d.year,
                "dia_semana_num":  dow,
                "dia_semana_nome": NOMES_DIAS[dow],
                "fim_de_semana":   dow >= 5,
            }
    return list(registros.values())



# LOAD
def conectar(host, port, db, user, password):
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    conn.autocommit = False
    return conn


def criar_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def truncar_tabelas(conn):
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE fato_pedido, dim_vendedor, dim_produto, "
            "dim_cliente, dim_data RESTART IDENTITY CASCADE;"
        )
    conn.commit()


def carregar_dim_data(conn, registros):
    sql = """
        INSERT INTO dim_data
            (id_data, data, dia, mes, nome_mes, trimestre, ano,
             dia_semana_num, dia_semana_nome, fim_de_semana)
        VALUES %s
        ON CONFLICT (id_data) DO NOTHING;
    """
    valores = [(
        r["id_data"], r["data"], r["dia"], r["mes"], r["nome_mes"],
        r["trimestre"], r["ano"], r["dia_semana_num"],
        r["dia_semana_nome"], r["fim_de_semana"],
    ) for r in registros]
    with conn.cursor() as cur:
        execute_values(cur, sql, valores)
    conn.commit()


def carregar_dim_cliente(conn, clientes):
    sql_ins = """
        INSERT INTO dim_cliente (customer_unique_id, cidade, estado)
        VALUES %s ON CONFLICT (customer_unique_id) DO NOTHING;
    """
    sql_sel = "SELECT id_cliente, customer_unique_id FROM dim_cliente;"
    valores = [
        (row["customer_unique_id"], row["cidade"] or None, row["estado"] or None)
        for _, row in clientes.iterrows()
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql_ins, valores)
        cur.execute(sql_sel)
        mapa = {row[1]: row[0] for row in cur.fetchall()}
    conn.commit()
    return mapa


def carregar_dim_produto(conn, produtos):
    sql_ins = """
        INSERT INTO dim_produto (product_id, categoria, categoria_en)
        VALUES %s ON CONFLICT (product_id) DO NOTHING;
    """
    sql_sel = "SELECT id_produto, product_id FROM dim_produto;"
    valores = [
        (row["product_id"], row["categoria"] or None, row["categoria_en"] or None)
        for _, row in produtos.iterrows()
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql_ins, valores)
        cur.execute(sql_sel)
        mapa = {row[1]: row[0] for row in cur.fetchall()}
    conn.commit()
    return mapa


def carregar_dim_vendedor(conn, vendedores):
    sql_ins = """
        INSERT INTO dim_vendedor (seller_id, cidade, estado)
        VALUES %s ON CONFLICT (seller_id) DO NOTHING;
    """
    sql_sel = "SELECT id_vendedor, seller_id FROM dim_vendedor;"
    valores = [
        (row["seller_id"], row["cidade"] or None, row["estado"] or None)
        for _, row in vendedores.iterrows()
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql_ins, valores)
        cur.execute(sql_sel)
        mapa = {row[1]: row[0] for row in cur.fetchall()}
    conn.commit()
    return mapa


def carregar_fato(conn, df, map_cliente, map_produto, map_vendedor):
    registros = []
    pulados   = 0

    for _, row in df.iterrows():
        id_data_compra  = int(row["data_compra"].strftime("%Y%m%d"))
        id_data_entrega = (
            int(row["data_entrega"].strftime("%Y%m%d"))
            if row["data_entrega"] else None
        )
        id_cliente  = map_cliente.get(row["customer_unique_id"])
        id_produto  = map_produto.get(row["product_id"])
        id_vendedor = map_vendedor.get(row["seller_id"])

        if None in (id_cliente, id_produto, id_vendedor):
            pulados += 1
            continue

        nota = _none(row.get("review_score"))
        nota = int(nota) if nota is not None else None

        parcelas = _none(row.get("parcelas"))
        parcelas = int(parcelas) if parcelas is not None else None

        registros.append((
            row["order_id"],
            id_data_compra,
            id_data_entrega,
            id_cliente,
            id_produto,
            id_vendedor,
            row["order_status"] or None,
            _none(row["valor_produto"]),
            _none(row["valor_frete"]),
            _none(row["valor_total"]),
            row.get("tipo_pagamento") or None,
            parcelas,
            _none(row.get("valor_pago")),
            nota,
            int(row["qtd_itens"]),
            _none(row["dias_para_entrega"]),
        ))

    sql = """
        INSERT INTO fato_pedido (
            order_id, id_data_compra, id_data_entrega,
            id_cliente, id_produto, id_vendedor,
            status_pedido, valor_produto, valor_frete, valor_total,
            tipo_pagamento, parcelas, valor_pago,
            nota_avaliacao, qtd_itens, dias_para_entrega
        ) VALUES %s;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, registros, page_size=500)
    conn.commit()

    if pulados:
        print(f"  Aviso: {pulados} linha(s) ignoradas (chave nao encontrada nas dimensoes)")

    return len(registros)



# EXECUÇÃO PRINCIPAL
def executar_etl(host, port, db, user, password, pasta):
    print("=" * 60)
    print("  ETL - DW Olist E-Commerce")
    print("=" * 60)

    print("\n[1/4] Extraindo CSVs...")
    dfs = extrair_csvs(pasta)

    print("\n[2/4] Transformando dados...")
    df, clientes, produtos, vendedores = transformar(dfs)
    print(f"  Total de itens de pedido apos joins: {len(df)}")

    print("\n[3/4] Conectando ao banco...")
    conn = conectar(host, port, db, user, password)

    try:
        criar_schema(conn)
        truncar_tabelas(conn)

        print("\n[4/4] Carregando dimensoes e fato...")

        todas_datas = (
            list(df["data_compra"].dropna().unique())
            + list(df["data_entrega"].dropna().unique())
        )
        carregar_dim_data(conn, construir_dim_data(todas_datas))
        print("  dim_data        OK")

        map_cliente  = carregar_dim_cliente(conn, clientes)
        print(f"  dim_cliente     OK  ({len(map_cliente)} clientes unicos)")

        map_produto  = carregar_dim_produto(conn, produtos)
        print(f"  dim_produto     OK  ({len(map_produto)} produtos)")

        map_vendedor = carregar_dim_vendedor(conn, vendedores)
        print(f"  dim_vendedor    OK  ({len(map_vendedor)} vendedores)")

        total_fato = carregar_fato(conn, df, map_cliente, map_produto, map_vendedor)
        print(f"  fato_pedido     OK  ({total_fato} registros)")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("  ETL concluido com sucesso!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL - DW Olist E-Commerce")
    parser.add_argument("--host",     default=os.getenv("DB_HOST",     "localhost"))
    parser.add_argument("--port",     default=int(os.getenv("DB_PORT", "5432")), type=int)
    parser.add_argument("--db",       default=os.getenv("DB_NAME",     "dw_olist"))
    parser.add_argument("--user",     default=os.getenv("DB_USER",     "postgres"))
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("--pasta",    default=os.getenv("DB_PASTA",    "./dados"))
    args = parser.parse_args()

    executar_etl(args.host, args.port, args.db, args.user, args.password, args.pasta)