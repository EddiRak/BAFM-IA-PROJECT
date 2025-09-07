## Exemples de fichiers d'entrée

### AIR
```plaintext
AIROUTPUTCDR_413_EC22.DetailOutputRecord.adjustmentRecordV2
{
  adjustmentType: '41232'D
  adjustmentAmount: '1000'D
  ...
}
```

### MSC
```plaintext
CME20MSC2013Aber.CallDataRecord.uMTSGSMPLMNCallDataRecord
{
  mSOriginating {
    callingNumber: "123456789"
    ...
  }
}
```

### SDP
```plaintext
SdpoutputCdr2_255_CS6.SDPCallDataRecord.accountAdjustment
{
  adjustmentDate: "20250820"
  adjustmentTime: "074407"
  ...
}
```

## Dépannage

### Erreurs courantes

1. **Fichiers non trouvés**
   - Vérifier les chemins dans les variables d'environnement
   - S'assurer que les fichiers sont dans les bons dossiers sous `raw_data/`

2. **Erreurs de parsing**
   - Vérifier le format des fichiers d'entrée
   - S'assurer que les séparateurs sont corrects (virgule pour OCC/CCN, pipe pour SGSN)
   - Valider l'encodage des fichiers (UTF-8 attendu)

3. **Erreurs de mémoire**
   - Réduire le nombre de workers Dask (variable DASK_WORKERS)
   - Augmenter la mémoire disponible
   - Traiter les fichiers par lots plus petits

4. **Fichiers Parquet corrompus**
   - Vérifier l'espace disque disponible
   - S'assurer que les permissions sont correctes
   - Relancer le traitement pour le module concerné

### Logs et débogage

Les logs sont essentiels pour le débogage :
- Consulter les logs dans la console pour voir les erreurs
- Vérifier les statistiques de traitement (fichiers réussis/échoués)
- Utiliser le notebook de vérification pour valider les données

### Solutions recommandées

1. **Performance**
   - Ajuster DASK_WORKERS selon les ressources disponibles
   - Traiter un sous-ensemble de fichiers pour tester
   - Monitorer l'utilisation mémoire

2. **Qualité des données**
   - Utiliser le notebook de vérification régulièrement
   - Vérifier les nombres d'enregistrements attendus
   - Valider les transformations de données

3. **Maintenance**
   - Nettoyer régulièrement les fichiers temporaires
   - Archiver les données traitées
   - Maintenir les logs à jour
