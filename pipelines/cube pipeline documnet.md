# SSAS Tabular Deployment Pipeline  
**Design & Operating Guide**

---

## 1. Purpose

This document describes the design and operating model for deploying **SSAS Tabular cubes** for the SimulationsAnalytics EDW using **Azure DevOps CI/CD pipelines**.

The goal of this pipeline is to provide:

- Controlled, repeatable deployments of SSAS Tabular models  
- Environment-specific deployments to **DEV, QAT, UAT, and PRD**  
- **Approval gates** in every environment  
- Full **traceability** between Git commits and deployed cube versions  
- A process consistent with the existing dbt deployment pipelines  

---

## 2. Guiding Principles

The SSAS deployment process is designed around the following principles:

- **Source-controlled deployments**  
  All deployments come from the `main` branch of the Git repository.

- **Host VM–based execution**  
  Each environment deploys from its own dedicated Host VM using a self-hosted Azure DevOps agent running under a service account.

- **One pipeline per environment**  
  DEV, QAT, UAT, and PRD each have their own deployment pipeline.

- **Manual approvals everywhere**  
  Every deployment requires an explicit approval before execution.

- **Short-lived pipeline runs**  
  Pipelines are not chained across environments to avoid long-running executions caused by QA cycles or CAB scheduling.

- **Model-centric artifact**  
  The authoritative deployment artifact is the **`.bim` model file**.

---

## 3. High-Level Architecture

### 3.1 Repository (Source of Truth)

- SSAS Tabular model stored in Git as a `.bim` file  
- All changes merged to `main` before deployment  
- `main` always represents the next deployable version of the cube  

---

### 3.2 Execution Hosts

Each environment has:

- A Windows Host VM  
- Azure DevOps self-hosted agent installed  
- Agent runs under a service account  
- Network access to its corresponding SQL/SSAS instance  

The VM is responsible for:

- Maintaining a persistent local clone of the repo  
- Pulling latest `main` during deployment  
- Executing the SSAS deployment tooling  

---

### 3.3 Deployment Tooling

The pipeline uses:

**Tabular Editor CLI**

Tabular Editor CLI is used to:

- Load the `.bim` model  
- Deploy the model to SSAS Tabular  
- Target a specific server and database  
- Support future enhancements such as:
  - environment overrides
  - best practice validation
  - scripted processing

This approach avoids dependency on Visual Studio / SSDT build artifacts and provides a clean, CI/CD-friendly deployment mechanism.

---

## 4. Environment Strategy

Each environment has its **own pipeline**:

| Environment | Trigger | Purpose |
|------------|----------|---------|
DEV | Automatic on `main` | Continuous deploy readiness and validation  
QAT | Manual | QA validation deployments  
UAT | Manual | User acceptance testing deployments  
PRD | Manual | CAB-controlled production deployments  

Each pipeline:

- Uses a **deployment job**  
- Targets an Azure DevOps **Environment + VM resource**  
- Requires **approval** before execution  

---

## 5. Trigger Strategy

### DEV pipeline

- Automatically triggered on:
  - commits to `main`
  - changes within cube-related paths

- Always requires manual approval before deployment.

This ensures:

- `main` is continuously validated as deployable  
- deployment issues are discovered early  
- DEV reflects the true state of `main`

---

### QAT / UAT / PRD pipelines

- No automatic trigger  
- Manually started when promotion is required  
- Always pull from `origin/main`

This ensures:

- promotion is intentional  
- QA and CAB timelines are not constrained by pipeline run lifetimes  
- environments remain fully independent

---

## 6. Deployment Flow (per pipeline run)

1. **Manual approval gate**
2. **Agent executes on target Host VM**
3. **VM-local repo update**
   - `git fetch --all --prune`
   - `git reset --hard origin/main`
4. **Model deployment**
   - Tabular Editor CLI loads the `.bim`
   - Model is deployed to SSAS Tabular
5. **(Optional / future)**
   - Model processing
   - Validation checks

---

## 7. Server & Database Conventions

### SSAS / SQL Server

Where `{env}` is:
- dev
- qat
- uat
- prd

### SSAS Database

---

## 8. Why This Model Was Chosen

This architecture was chosen because it:

- Matches the established dbt deployment approach  
- Avoids long-running, multi-day pipeline executions  
- Supports formal QA and CAB processes  
- Enables clean auditing and rollback  
- Minimizes tooling complexity  
- Scales well as cube complexity increases  

---

## 9. Future Enhancements

- XMLA artifact generation and archiving  
- Environment parameterization  
- Best practice analyzer checks  
- Automated processing strategies  
- Deployment logging  

---

## 10. Summary

The SSAS Tabular deployment pipeline provides a controlled, repeatable, and auditable method for promoting semantic model changes across environments. It intentionally favors **environment independence and governance** over full automation, aligning with enterprise QA and CAB workflows while still providing modern CI/CD reliability.
