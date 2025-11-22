# Lab Metabase – Data Warehouse Corporativo

Este repositório contém um pequeno Data Warehouse construído a partir de **duas fontes transacionais**:

- Banco **Northwind** (loja fictícia de comércio internacional)
- Banco **Metabase Sample** (e-commerce fictício)

Além disso, o DW integra uma fonte **externa de câmbio USD/BRL**, permitindo análises que combinam vendas com contexto macroeconômico.

O foco do projeto é demonstrar:

- Modelagem **dimensional** (fatos e dimensões)
- Uso de **dbt** para ETL/ELT
- Integração de múltiplas fontes heterogêneas
- Consumo dos dados em **Metabase** via dashboards.

---

## Arquitetura em camadas

No schema `public` do banco `dw` temos:

### **Raw/Ext**
- `ext_northwind.*`
- `ext_metabase.*`
- `dw_ext_economia.usd_brl_raw` (câmbio USD/BRL bruto)

### **Staging (`models/staging`)**
- Padronização de nomes, limpeza de formatos e tipos
- Exemplos:  
  `stg_northwind_orders`,  
  `stg_metabase_orders`,  
  `stg_usd_brl` (tratamento de data e taxa de câmbio)

### **Intermediate (`models/intermediate`)**
- Integração das origens Northwind + Metabase em tabelas unificadas:
  - `int_clientes`
  - `int_produtos`
  - `int_vendas`

### **Dimensões (`models/marts/dim`)**
- `dim_cliente`
- `dim_produto`
- `dim_tempo`
- `dim_cambio`

### **Fato (`models/marts/fact`)**
- `fact_vendas` – fato transacional consolidada

### **Data Marts (`models/marts/dm_*`)**
- `fact_clientes_resumo_mensal`
- `fact_produtos_resumo_mensal`

---

## Pré-requisitos

- **Docker** e **Docker Compose**
- **Python 3.11+**
- **dbt-postgres 1.10.x**

Instalação do dbt:

```bash
pip install "dbt-postgres==1.10.*"
```

---

## Como subir a infraestrutura

Na raiz do projeto (`lab-metabase/`):

```bash
docker compose up -d
```

Serviços expostos:

- Postgres → `localhost:5435`  
- Metabase → `http://localhost:3000`

---

## Como rodar o dbt

1. Acesse o diretório do dbt:
   ```bash
   cd dw_labs
   ```

2. Teste o perfil:
   ```bash
   dbt debug
   ```

3. Rode as seeds:
   ```bash
   dbt seed --full-refresh
   ```

4. Construa o DW:
   ```bash
   dbt run
   ```

---

## Como usar no Metabase

1. Abra `http://localhost:3000`
2. Conecte ao Postgres (`dw`, porta `5435`)
3. Utilize as tabelas:
   - `dim_*`
   - `fact_*`
   - `dm_*`

---

## Estrutura de pastas

```
lab-metabase/
├─ docker-compose.yml
├─ sql/
└─ dw_labs/
   ├─ dbt_project.yml
   ├─ models/
   ├─ seeds/
   ├─ logs/
   └─ target/
```

---

## Licença

Projeto acadêmico focado em práticas de DW, dbt e Metabase.
