# Projet de Traitement ETL des CDR/EDR

Ce projet implémente un pipeline ETL (Extract, Transform, Load) pour traiter différents types de fichiers CDR (Call Detail Records) et EDR (Event Detail Records) provenant de différents systèmes de télécommunication.

## 📋 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Structure du projet](#structure-du-projet)
3. [Types de données traités](#types-de-données-traités)
4. [Configuration requise](#configuration-requise)
5. [Installation](#installation)
6. [Utilisation](#utilisation)
7. [Détails techniques](#détails-techniques)
8. [Validation des données](#validation-des-données)

## Vue d'ensemble

Le pipeline ETL traite les données de plusieurs systèmes :
- AIR (Account Information Records)
- MSC (Mobile Switching Center)
- SDP (Service Delivery Platform)
- SGSN (Serving GPRS Support Node)
- OCC (Online Charging Center)
- CCN (Call Control Node)

Chaque système génère des fichiers dans un format spécifique qui sont transformés en fichiers Parquet pour une analyse ultérieure.

## Structure du projet

```
CDR_EDR_unzipped/
├── requirements.txt
├── src/
│   └── etl/
│       ├── modules              # modules utilisés
│       ├── main_etl.py          # Point d'entrée principal
│       ├── air_etl.py           # Traitement AIR
│       ├── msc_etl.py           # Traitement MSC
│       ├── sdp_etl.py           # Traitement SDP
│       ├── sgsn_etl.py          # Traitement SGSN
│       ├── occ_etl.py           # Traitement OCC
│       └── ccn_etl.py           # Traitement CCN
├── raw_data/                    # Données brutes
│   ├── AIR/
│   ├── MSC/
│   ├── SDP/
│   ├── SGSN/
│   ├── OCC/
│   └── CCN/
└── processed_data/              # Données transformées
    └── ETL/
        ├── AIR/
        ├── MSC/
        ├── SDP/
        ├── SGSN/
        ├── OCC/
        └── CCN/
```

## Types de données traités

### AIR
- **Format d'entrée** : Fichiers .AIR ou .AIR.gz
- **Types d'enregistrements** : 
  - ADJUSTMENT (adjustmentRecordV2)
  - REFILL (refillRecordV2)

### MSC
- **Format d'entrée** : Fichiers TTFILE
- **Services traités** :
  - mSOriginatingSMSinMSC
  - mSTerminatingSMSinMSC
  - mSTerminating
  - sSProcedure
  - transit
  - mSOriginating
  - callForwarding

### SDP
- **Format d'entrée** : Fichiers .ASN ou .ASN.gz
- **Types d'enregistrements** :
  - ACCOUNT_ADJUSTMENT
  - PERIODIC_ACCOUNT_MGMT
  - LIFE_CYCLE_CHANGE

### SGSN
- **Format d'entrée** : Fichiers MGANPGW
- **Séparateur** : |
- **Format de sortie** : Un fichier Parquet unique

### OCC
- **Format d'entrée** : Fichiers .ber, .ber_NP (DATA) et .ber-SMS (SMS)
- **Séparateur** : ,
- **Sorties distinctes** : 
  - DATA (occ_data.parquet)
  - SMS (occ_sms.parquet)

### CCN
- **Format d'entrée** : Fichiers CCNCDR
- **Séparateur** : ,
- **Format de sortie** : Un fichier Parquet unique

## Configuration requise

- Python 3.8+
- pandas
- dask
- pyarrow (pour le format Parquet)
- gzip (pour la décompression)

## Installation

1. Cloner le repository :
```bash
git clone https://github.com/EddiRak/BAFM-IA-PROJECT.git
cd CDR_EDR_unzipped
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

## Utilisation

1. Placer les fichiers source dans les dossiers appropriés sous `raw_data/`

2. Exécuter le pipeline complet :
```bash
python src/etl/main_etl.py
```

Ou exécuter un module spécifique :
```bash
python src/etl/air_etl.py  # Pour AIR uniquement
python src/etl/msc_etl.py  # Pour MSC uniquement
# etc.
```

## Détails techniques

### Traitement parallèle
- Utilisation de Dask pour le traitement parallèle
- Nombre de workers configurable (par défaut : 4)
- Variable d'environnement : DASK_WORKERS

### Gestion des fichiers
- Support des fichiers compressés (.gz)
- Détection automatique du format
- Gestion robuste des erreurs

### Logging
Format unifié pour tous les modules :
```
INFO     - ETL XXX - Démarrage
INFO     - • Date      : YYYY-MM-DD HH:MM:SS
INFO     - • Source    : <dossier_source>
INFO     - • Cible     : <dossier_cible>
...
INFO     - ======================================
INFO     - Résultats du traitement:
INFO     - • Fichiers traités avec succès : X
INFO     - • Fichiers en erreur          : Y
INFO     - • Total des enregistrements   : Z
INFO     - • Temps d'exécution           : T.tt secondes
INFO     - ======================================
```

## Validation des données

Pour vérifier les données transformées, utiliser le notebook `verification.ipynb` qui permet de :
- Visualiser les données de chaque système
- Vérifier les nombres d'enregistrements
- Examiner la structure des données
- Valider les transformations

## Variables d'environnement

Les chemins peuvent être configurés via des variables d'environnement :
- RAW_DATA_PATH
- PROCESSED_DATA_PATH
- DASK_WORKERS

Par défaut, les chemins relatifs sont utilisés si les variables ne sont pas définies.
