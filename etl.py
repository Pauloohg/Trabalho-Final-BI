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