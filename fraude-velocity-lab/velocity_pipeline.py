#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fraude Velocity Lab — Pipeline principal.

Calcula métricas de "velocity" por cliente a partir de pedidos (demo sintético
ou MySQL). Gera um CSV com uma linha por cliente e colunas agregadas por
janelas (1d, 7d, 30d), além de um score didático.

Modos:
- demo  : gera dados sintéticos e calcula as features
- mysql : lê views auxiliares no MySQL e calcula as features

Saída:
- CSV com uma linha por cliente (padrão: sample_output/velocity_features.csv)

Uso (CLI):
    python velocity_pipeline.py --mode demo  --out sample_output/velocity.csv
    python velocity_pipeline.py --mode mysql --out sample_output/velocity.csv

MySQL (modo mysql):
- Requer as variáveis de ambiente:
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

Dependências:
- numpy, pandas
- (modo mysql) sqlalchemy, pymysql
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def _print(msg: str) -> None:
    """Imprime mensagem com timestamp (uso simples de logging)."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# -------------------------------
# Helpers
# -------------------------------
def ensure_views_mysql(engine) -> None:
    """Cria ou recria views auxiliares necessárias no MySQL.

    Views:
        - v_order_payment_total: soma value por order_id.
        - v_orders_enriched    : pedido + cliente + ts + total pago.

    Observação:
        Ajuste os nomes das tabelas base conforme o schema do seu banco.
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


def load_orders_demo(n_customers: int = 50, days: int = 45, seed: int = 42) -> pd.DataFrame:
    """Gera amostra sintética de pedidos para experimentação local.

    Args:
        n_customers: número de clientes simulados.
        days: janela de datas para espalhar os pedidos.
        seed: semente para reprodutibilidade.

    Returns:
        DataFrame com colunas: order_id, customer_unique_id,
        order_purchase_timestamp, payment_total.
    """
    rng = np.random.default_rng(seed)
    now = pd.Timestamp.utcnow().floor("s")

    customers = [f"C{i:04d}" for i in range(n_customers)]
    rows = []
    order_id = 1
    for cust in customers:
        n_orders = rng.integers(1, 12)  # 1 a 11 pedidos por cliente
        for _ in range(n_orders):
            delta_days = int(rng.integers(0, days))
            delta_secs = int(rng.integers(0, 24 * 3600))
            ts = now - pd.Timedelta(days=delta_days, seconds=delta_secs)
            value = float(np.round(rng.uniform(20, 800), 2))
            rows.append((f"O{order_id:06d}", cust, ts.tz_localize(None), value))
            order_id += 1

    df = pd.DataFrame(
        rows,
        columns=[
            "order_id",
            "customer_unique_id",
            "order_purchase_timestamp",
            "payment_total",
        ],
    )
    return df.sort_values("order_purchase_timestamp").reset_index(drop=True)


def load_orders_mysql() -> pd.DataFrame:
    """Lê dados das views no MySQL usando sqlalchemy+pymysql.

    Espera variáveis de ambiente:
        MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

    Returns:
        DataFrame com colunas: order_id, customer_unique_id,
        order_purchase_timestamp, payment_total (últimos 60 dias).
    """
    from sqlalchemy import create_engine, text  # noqa: F401

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

    # Tipos e limpeza mínima
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    )
    df = df.dropna(subset=["order_purchase_timestamp"])
    return df


def compute_velocity_features(
    df_orders: pd.DataFrame, now_ts: pd.Timestamp | None = None
) -> pd.DataFrame:
    """Calcula métricas de velocity e um score didático por cliente.

    Entradas esperadas em df_orders:
        - customer_unique_id (str)
        - order_purchase_timestamp (datetime-like)
        - payment_total (numérico)

    Métricas:
        - orders_{1,7,30}d / value_{1,7,30}d / avg_ticket_{7,30}d
        - interpurchase_hours (média das diferenças entre compras)
        - velocity_score (0–100; combinação min-max de componentes)

    Args:
        df_orders: pedidos (uma linha por order_id).
        now_ts: referência temporal (default = UTC now sem tz).

    Returns:
        DataFrame por cliente, ordenado por velocity_score desc.
    """
    if now_ts is None:
        now_ts = pd.Timestamp.utcnow().tz_localize(None)

    df = df_orders.copy()
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    )
    df["payment_total"] = pd.to_numeric(df["payment_total"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["customer_unique_id", "order_purchase_timestamp"])

    def window_mask(days: int) -> pd.Series:
        return df["order_purchase_timestamp"] >= (now_ts - pd.Timedelta(days=days))

    def agg_window(days: int) -> pd.DataFrame:
        d = df[window_mask(days)]
        g = d.groupby("customer_unique_id").agg(
            orders=("order_id", "count"),
            value=("payment_total", "sum"),
            avg_ticket=("payment_total", "mean"),
        )
        return g.rename(
            columns={
                "orders": f"orders_{days}d",
                "value": f"value_{days}d",
                "avg_ticket": f"avg_ticket_{days}d",
            }
        )

    g1 = agg_window(1)
    g7 = agg_window(7)
    g30 = agg_window(30)

    # Intervalo médio entre compras (em horas)
    df_sorted = df.sort_values(["customer_unique_id", "order_purchase_timestamp"])
    df_sorted["prev_ts"] = df_sorted.groupby("customer_unique_id")[
        "order_purchase_timestamp"
    ].shift(1)
    df_sorted["delta_h"] = (
        df_sorted["order_purchase_timestamp"] - df_sorted["prev_ts"]
    ).dt.total_seconds() / 3600.0
    inter = (
        df_sorted.groupby("customer_unique_id")["delta_h"]
        .mean()
        .to_frame("interpurchase_hours")
    )

    # Base de clientes (garante presença de todos)
    base = (
        pd.Index(df["customer_unique_id"].unique(), name="customer_unique_id")
        .to_frame(index=False)
        .set_index("customer_unique_id")
    )
    out = base.join([g1, g7, g30, inter], how="left").fillna(0.0)

    # Score didático: combinação linear com normalização min-max
    def minmax(s: pd.Series) -> pd.Series:
        span = s.max() - s.min()
        if span <= 0:
            return pd.Series(np.zeros_like(s), index=s.index, dtype=float)
        return (s - s.min()) / span

    score = (
        0.30 * minmax(out["orders_7d"])
        + 0.25 * minmax(out["value_7d"])
        + 0.20 * minmax(out["orders_30d"])
        + 0.15 * minmax(out["value_30d"])
        # Quanto menor o intervalo médio entre compras, maior a “velocity”
        + 0.10 * (1 - minmax(out["interpurchase_hours"].replace(0, out["interpurchase_hours"].max())))
    )
    out["velocity_score"] = (score * 100).round(2)

    out = out.sort_values("velocity_score", ascending=False).reset_index()

    cols = [
        "customer_unique_id",
        "orders_1d",
        "value_1d",
        "orders_7d",
        "value_7d",
        "avg_ticket_7d",
        "orders_30d",
        "value_30d",
        "avg_ticket_30d",
        "interpurchase_hours",
        "velocity_score",
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = 0.0
    return out[cols]


def main() -> None:
    """Interface de linha de comando simples do pipeline."""
    ap = argparse.ArgumentParser(description="Fraude Velocity Lab — pipeline")
    ap.add_argument(
        "--mode",
        choices=["demo", "mysql"],
        default="demo",
        help="Modo de execução.",
    )
    ap.add_argument(
        "--out",
        default="sample_output/velocity_features.csv",
        help="Caminho do CSV de saída.",
    )
    args = ap.parse_args()

    if args.mode == "demo":
        _print("Gerando dados sintéticos...")
        df_orders = load_orders_demo(n_customers=60, days=60, seed=2025)
    else:
        try:
            from sqlalchemy import create_engine  # noqa: F401
            import pymysql  # noqa: F401
        except Exception:
            _print(
                "Erro: modo mysql requer 'sqlalchemy' e 'pymysql'. "
                "Instale com: pip install -r requirements.txt"
            )
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

