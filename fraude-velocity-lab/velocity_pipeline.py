#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fraude Velocity Lab — Pipeline principal

Modos:
- demo  : gera dados sintéticos e calcula as features
- mysql : conecta no MySQL, cria views auxiliares e calcula as features

Saída: CSV com uma linha por cliente
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

def _print(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# -------------------------------
# Helpers
# -------------------------------
def ensure_views_mysql(engine):
    """
    Cria ou recria as views auxiliares:
      - v_order_payment_total
      - v_orders_enriched
    Ajuste os nomes das tabelas base conforme seu schema.
    """
    create_payment_total = """
    CREATE OR REPLACE VIEW v_order_payment_total AS
    SELECT
        op.order_id,
        SUM(op.payment_value) AS payment_total
    FROM order_payments op
    GROUP BY op.order_id;
    """

    create_orders_enriched = """
    CREATE OR REPLACE VIEW v_orders_enriched AS
    SELECT
        o.order_id,
        o.customer_unique_id,
        o.order_purchase_timestamp,
        pt.payment_total
    FROM orders o
    INNER JOIN v_order_payment_total pt USING (order_id);
    """
    with engine.begin() as conn:
        conn.execute(create_payment_total)
        conn.execute(create_orders_enriched)
    _print("Views criadas/atualizadas: v_order_payment_total, v_orders_enriched")

def load_orders_demo(n_customers=50, days=45, seed=42) -> pd.DataFrame:
    """
    Gera uma amostra de pedidos sintéticos: cada pedido tem (cliente, ts, valor).
    """
    rng = np.random.default_rng(seed)
    now = pd.Timestamp.utcnow().floor('s')

    customers = [f"C{i:04d}" for i in range(n_customers)]
    rows = []
    order_id = 1
    for cust in customers:
        # cada cliente faz entre 1 e 12 compras no período
        n_orders = rng.integers(1, 12)
        for _ in range(n_orders):
            # espalhar as compras no intervalo (days)
            delta_days = int(rng.integers(0, days))
            delta_secs = int(rng.integers(0, 24*3600))
            ts = now - pd.Timedelta(days=delta_days, seconds=delta_secs)
            value = float(np.round(rng.uniform(20, 800), 2))
            rows.append((f"O{order_id:06d}", cust, ts.tz_localize(None), value))
            order_id += 1

    df = pd.DataFrame(rows, columns=["order_id", "customer_unique_id", "order_purchase_timestamp", "payment_total"])
    return df.sort_values("order_purchase_timestamp").reset_index(drop=True)

def load_orders_mysql() -> pd.DataFrame:
    """
    Lê as views do MySQL com sqlalchemy+pymysql (requer variáveis de ambiente).
    Espera:
      - MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
    """
    from sqlalchemy import create_engine, text

    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    db = os.getenv("MYSQL_DB", "olist_nolan")

    uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(uri, pool_pre_ping=True)

    ensure_views_mysql(engine)

    query = """
    SELECT
        order_id,
        customer_unique_id,
        order_purchase_timestamp,
        payment_total
    FROM v_orders_enriched
    WHERE order_purchase_timestamp >= NOW() - INTERVAL 60 DAY
    """
    _print("Lendo v_orders_enriched (últimos 60 dias)...")
    df = pd.read_sql(query, con=engine)
    # Garantir tipos
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df = df.dropna(subset=["order_purchase_timestamp"])
    return df

def compute_velocity_features(df_orders: pd.DataFrame, now_ts: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Recebe um DF de pedidos com colunas:
      - customer_unique_id
      - order_purchase_timestamp
      - payment_total
    Retorna um DF por cliente com métricas de velocity.
    """
    if now_ts is None:
        now_ts = pd.Timestamp.utcnow().tz_localize(None)

    # garantir tipos
    df = df_orders.copy()
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df["payment_total"] = pd.to_numeric(df["payment_total"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["customer_unique_id", "order_purchase_timestamp"])

    # janelas
    def window_mask(days):
        return df["order_purchase_timestamp"] >= (now_ts - pd.Timedelta(days=days))

    # agregações por cliente
    def agg_window(days):
        d = df[window_mask(days)]
        g = d.groupby("customer_unique_id").agg(
            orders=( "order_id", "count"),
            value =("payment_total", "sum"),
            avg_ticket=("payment_total", "mean"),
        )
        g = g.rename(columns={
            "orders": f"orders_{days}d",
            "value": f"value_{days}d",
            "avg_ticket": f"avg_ticket_{days}d",
        })
        return g

    g1  = agg_window(1)
    g7  = agg_window(7)
    g30 = agg_window(30)

    # interpurchase_hours
    df_sorted = df.sort_values(["customer_unique_id", "order_purchase_timestamp"])
    df_sorted["prev_ts"] = df_sorted.groupby("customer_unique_id")["order_purchase_timestamp"].shift(1)
    df_sorted["delta_h"] = (df_sorted["order_purchase_timestamp"] - df_sorted["prev_ts"]).dt.total_seconds() / 3600.0
    inter = df_sorted.groupby("customer_unique_id")["delta_h"].mean().to_frame("interpurchase_hours")

    # juntar
    base = pd.Index(df["customer_unique_id"].unique(), name="customer_unique_id").to_frame(index=False).set_index("customer_unique_id")
    out = base.join([g1, g7, g30, inter], how="left").fillna(0.0)

    # score didático: normalização min-max de alguns componentes
    def minmax(s: pd.Series) -> pd.Series:
        if (s.max() - s.min()) <= 0:
            return pd.Series(np.zeros_like(s), index=s.index, dtype=float)
        return (s - s.min()) / (s.max() - s.min())

    score = (
        0.30 * minmax(out["orders_7d"]) +
        0.25 * minmax(out["value_7d"]) +
        0.20 * minmax(out["orders_30d"]) +
        0.15 * minmax(out["value_30d"]) +
        0.10 * (1 - minmax(out["interpurchase_hours"].replace(0, out["interpurchase_hours"].max())))
    )
    out["velocity_score"] = (score * 100).round(2)

    # ordenar por score desc
    out = out.sort_values("velocity_score", ascending=False).reset_index()

    # colunas ordenadas
    cols = [
        "customer_unique_id",
        "orders_1d","value_1d",
        "orders_7d","value_7d","avg_ticket_7d",
        "orders_30d","value_30d","avg_ticket_30d",
        "interpurchase_hours",
        "velocity_score",
    ]
    # algumas podem não existir se não houve pedidos na janela; garantir presença
    for c in cols:
        if c not in out.columns:
            out[c] = 0.0
    return out[cols]

def main():
    ap = argparse.ArgumentParser(description="Fraude Velocity Lab — pipeline")
    ap.add_argument("--mode", choices=["demo", "mysql"], default="demo", help="Modo de execução.")
    ap.add_argument("--out", default="sample_output/velocity_features.csv", help="Caminho do CSV de saída.")
    args = ap.parse_args()

    if args.mode == "demo":
        _print("Gerando dados sintéticos...")
        df_orders = load_orders_demo(n_customers=60, days=60, seed=2025)
    else:
        try:
            from sqlalchemy import create_engine  # noqa: F401
            import pymysql  # noqa: F401
        except Exception as e:
            _print("Erro: modo mysql requer 'sqlalchemy' e 'pymysql'. Instale com: pip install -r requirements.txt")
            sys.exit(2)
        _print("Lendo pedidos do MySQL...")
        df_orders = load_orders_mysql()

    _print(f"{len(df_orders)} linhas de pedidos carregadas.")
    _print("Calculando features de velocity...")
    features = compute_velocity_features(df_orders)

    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    features.to_csv(out_path, index=False)
    _print(f"Arquivo salvo em: {out_path}")

if __name__ == "__main__":
    main()
