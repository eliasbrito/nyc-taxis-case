# 100 - Databricks (Databricks Infrastructure)

## 📌 Descrição

Esta camada é responsável por provisionar a infraestrutura base no Databricks Community Terraform.

Os recursos criados aqui são:

* Catalog Schemas **Silver e Gold**
* External Loations para comunicação com buckets S3 **Bronze, Silver e Gold**

---

## 🧱 Estrutura

```
100-databricks/
 ├── main.tf
 ├── variables.tf
 ├── providers.tf
 └── terraform.tfvars (dados sensíveis - NÃO VERSIONAR)
```

---

## ⚙️ Pré-requisitos

* Terraform instalado
* AWS CLI configurado (`aws configure`)
* PAT criado

---

## 🚀 Execução

### 1. Inicializar Terraform

```
terraform init
```

### 2. Planejar execução

```
terraform plan
```

### 3. Aplicar infraestrutura

```
terraform apply
```
---


## 📌 Responsabilidade da camada

✔ Infraestrutura base (Databricks)
