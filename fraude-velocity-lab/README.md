# Fraude Velocity Lab

Pipeline em **Python** para cálculo de métricas de *velocity* aplicadas à prevenção a fraudes.
Mostra como enriquecer pedidos, criar *features* por cliente e gerar um `velocity_score` didático.

> 💡 **Dois modos de uso**
> - **Demo (sem banco):** gera dados sintéticos e calcula as métricas.
> - **MySQL (produção):** conecta no MySQL, cria *views* auxiliares e calcula as métricas a partir de `orders` e `order_payments`.

---

## 🧱 Arquitetura resumida

1) **Ingestão**
   - (Demo) Gera dados sintéticos.
   - (MySQL) Lê tabelas, cria as *views* auxiliares:
     - `v_order_payment_total`: soma os pagamentos por pedido
     - `v_orders_enriched`: pedido + `customer_unique_id` + `order_purchase_timestamp` + valor total pago

2) **Feature store (por cliente)**
   - `orders_1d`, `orders_7d`, `orders_30d`
   - `value_1d`, `value_7d`, `value_30d`
   - `avg_ticket_7d`, `avg_ticket_30d`
   - `interpurchase_hours` (média de horas entre compras)

3) **Score didático**
   - Combinação simples, escalonada, para fins educacionais.

4) **Saída**
   - `sample_output/velocity_features_*.csv`

---

## 🚀 Como rodar

### Opção A) Modo Demo (sem banco)
Cria dados fictícios e salva um CSV com as *features* calculadas.

```bash
python velocity_pipeline.py --mode demo --out sample_output/velocity_features_demo.csv
```

### Opção B) Modo MySQL (produção)
Usa variáveis de ambiente para a conexão:

```bash
# Exemplo (Linux/macOS)
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=xxxxxxxx
export MYSQL_DB=olist_nolan

python velocity_pipeline.py --mode mysql --out sample_output/velocity_features_mysql.csv
```

No Windows (PowerShell):
```powershell
$Env:MYSQL_HOST="127.0.0.1"
$Env:MYSQL_PORT="3306"
$Env:MYSQL_USER="root"
$Env:MYSQL_PASSWORD="xxxxxxxx"
$Env:MYSQL_DB="olist_nolan"

python velocity_pipeline.py --mode mysql --out sample_output/velocity_features_mysql.csv
```

---

## 📦 Requisitos

```bash
pip install -r requirements.txt
```

- Python 3.10+
- MySQL com tabelas `orders`, `order_payments` (ajuste nomes se necessário).

---

## 🧪 Colunas esperadas (MySQL)

- **orders**: `order_id`, `customer_unique_id`, `order_purchase_timestamp`
- **order_payments**: `order_id`, `payment_value`

> Se seus nomes diferirem, ajuste os SELECTs no arquivo `velocity_pipeline.py`.

---

## 📝 Exemplo de saída

Um CSV com uma linha por **cliente**, incluindo as métricas e o `velocity_score`. Um exemplo gerado em `sample_output/velocity_features_demo.csv` já acompanha o repositório.

---

## ♻️ Estrutura do projeto

```
fraude-velocity-lab/
├── LICENSE
├── README.md
├── requirements.txt
├── .gitignore
├── velocity_pipeline.py
└── sample_output/
    └── velocity_features_demo.csv
```

---

## ⚠️ Avisos

- O `velocity_score` é **didático**. Adapte pesos/normalizações para seu caso real.
- Cuidado com dados sensíveis: não suba credenciais; use variáveis de ambiente.

---

## 📄 Licença

MIT — veja `LICENSE`.
