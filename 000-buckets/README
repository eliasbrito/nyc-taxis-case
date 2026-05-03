# 000 - Buckets (AWS Infrastructure)

## 📌 Descrição

Esta camada é responsável por provisionar a infraestrutura base na AWS utilizando Terraform.

Os recursos criados aqui são:

* Bucket S3 para **Terraform State**
* Tabela DynamoDB para **state locking**
* Buckets do Data Lake:

  * Bronze
  * Silver
  * Gold

Essa é a **primeira etapa obrigatória** da infraestrutura, pois o backend remoto do Terraform depende desses recursos.

---

## 🧱 Estrutura

```
000-buckets/
 ├── main.tf
 ├── variables.tf
 ├── outputs.tf
 ├── backend.tf (criado no passo seguinte)
 └── terraform.tfvars   
```

---

## ⚙️ Pré-requisitos

* Terraform instalado
* AWS CLI configurado (`aws configure`)
* Permissões para criar:

  * S3
  * DynamoDB

---

## 🚀 Execução (Primeira vez - Bootstrap)

> ⚠️ IMPORTANTE: Nesta etapa o backend ainda é **local**

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

Isso criará:

* Bucket de state
* DynamoDB (lock)
* Buckets bronze/silver/gold

---

## 🔐 Configuração do Backend Remoto

Após a criação dos recursos, crie o arquivo:

### `backend.tf`

```hcl
terraform {
  backend "s3" {
    bucket         = "<SEU_BUCKET_DE_STATE>"
    key            = "global/terraform.tfstate"
    region         = "<REGIAO_DO_BUCKET>"
    dynamodb_table = "terraform-lock"
  }
}
```

---

### Reconfigurar o backend

```
terraform init -reconfigure
```

---

## 📦 Variáveis

As variáveis devem ser definidas via:

### Opção 1 — `terraform.tfvars`

```hcl
tf_state_bucket_name = "state-nyc-taxis-case"
bronze_bucket_name   = "bronze-taxis-nycs-case"
silver_bucket_name   = "silver-taxis-nycs-case"
gold_bucket_name     = "gold-taxis-nycs-case"
```

---

## 📤 Outputs

Após o apply, os seguintes valores são expostos:

* Nome dos buckets (bronze, silver, gold)
* Nome do bucket de state
* Nome da tabela DynamoDB

---

## 🧠 Observações

* O backend remoto **não pode ser usado antes de ser criado**
* Sempre garantir que a **região do backend** está correta
* Buckets S3 precisam ter nomes **globalmente únicos**

---

## 🔄 Fluxo esperado

1. Executar esta camada (`000-buckets`)
2. Configurar backend remoto
3. Reconfigurar Terraform
4. Seguir para próxima camada:

```
100-databricks/
```

---

## 📌 Responsabilidade da camada

✔ Infraestrutura base (AWS)
❌ Nenhum recurso do Databricks

---

## 👣 Próximo passo

Provisionar:

* Storage Credentials
* External Locations
* Unity Catalog (catalog, schemas, grants)

Na camada:

```
100-databricks
```
