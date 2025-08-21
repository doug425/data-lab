# 🧪 Data Lab — Fraud & Analytics  

Repositório de estudos e experimentos em **SQL, Python e Machine Learning** aplicados à **prevenção de fraudes**.  
Aqui documento minhas práticas, simulações e análises, sempre com foco em **casos reais de e-commerce**.

---

## 📂 Estrutura
- **SQL/** → queries para análise de pedidos, faturamento e vínculos  
- **Python/** → scripts de análise e modelos de ML (ex: velocity score, detecção de anomalias)  
- **Docs/** → anotações, relatórios e apresentações  

---

## 🎯 Objetivo
Consolidar aprendizados e construir soluções que simulem cenários de **prevenção a fraude em e-commerce**, incluindo:  
- Monitoramento de pedidos suspeitos  
- Criação de indicadores de *velocity*  
- Testes com algoritmos de ML para prever risco  

---

## ⚙️ Como usar
1. Clone o repositório:
   ```bash
   git clone https://github.com/doug425/data-lab.git

2. Execute os scripts Python:
   ```bash
   python nome_do_script.py

## 📊 Projetos em Python

- [Fraude Velocity Lab](fraude-velocity-lab)  
  Pipeline em Python para cálculo de métricas de *velocity* aplicadas à prevenção a fraudes (modo demo com dados sintéticos e modo MySQL com views auxiliares).  
  Inclui:
  - Geração de métricas como `orders_7d`, `avg_ticket_30d`, `interpurchase_hours`
  - Score didático de velocity
  - Exemplo pronto em `sample_output/velocity_features_demo.csv`


