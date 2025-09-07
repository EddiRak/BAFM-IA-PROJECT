# src/etl/modules/headers.py
from __future__ import annotations
from typing import Dict, List

# En-têtes EXACTS fournis (espaces superflus nettoyés)

OCC_HEADERS: List[str] = [
    "aNumber","bNumber","callStartDate","callStartTime","timeZone","duration","currencyType","cost",
    "dedicatedAccUsed","balanceAfter","trafficCase","teleServiceCode","location","dataVolume","numberOfEvents",
    "faFIndicator","networkID","serviceProviderID","serviceClass","accountGroupID","serviceOfferings",
    "selectedCommunityID","cellID","zoneType","zoneID1","zoneID2","zoneID3","dAIDs_Used","dA_Change","dA_valAfter",
    "accumulatorIDs","accumulator_Change","accumulator_valAfter","mccmnc","sgsnAddress","chargingID","aimsi","dAtype",
    "dAconnectedOfferID","subDAlist","subDAbalanceBefore","subDAbalanceAfter","subDAchange","subDAstartDate",
    "subDAexpiryDate","uClist","uCtype","uCvalueBefore","uCvalueAfter","uCchange","usedOfferID","pAMData",
    "mAconsumption","uAMA","uCMA","dAconsumption","uADA","ucDA","bonusMA","bonusDA","bonusSubDA","bonusUA",
    "bonusSupervisionExp","bonusServiceFeeExp","bonusCreditClearance","bonusServiceRemoval","bonusOffer","FileSourceName"
]

CCN_HEADERS: List[str] = [
    "aNumber","bNumber","callStartDate","callStartTime","timeZone","duration","currencyType","cost",
    "dedicatedAccUsed","balanceAfter","trafficCase","teleServiceCode","location","dataVolume","numberOfEvents",
    "faFIndicator","networkID","serviceProviderID","serviceClass","accountGroupID","serviceOfferings",
    "selectedCommunityID","cellID","zoneType","zoneID1","zoneID2","zoneID3","dAIDs_Used","dA_Change","dA_valAfter",
    "accumulatorIDs","accumulator_Change","accumulator_valAfter","dAtype","dAconnectedOfferID","subDAlist",
    "subDAbalanceBefore","subDAbalanceAfter","subDAchange","subDAstartDate","subDAexpiryDate","uClist","uCtype",
    "uCvalueBefore","uCvalueAfter","uCchange","usedOfferID","pAMData","mAconsumption","uAMA","uCMA","dAconsumption",
    "uADA","ucDA","bonusMA","bonusDA","bonusSubDA","bonusUA","bonusSupervisionExp","bonusServiceFeeExp",
    "bonusCreditClearance","bonusServiceRemoval","bonusOffer","mccmnc","FileSourceName"
]

SGSN_HEADERS: List[str] = [
    "recordType","servedIMSI","p-GWAddress","chargingID","servingNodeAddress","accessPointNameNI","pdpPDNType",
    "servedPDPPDNAddress","dynamicAddressFlag","qosNegotiated","dataVolumeGPRSUplink","dataVolumeGPRSDownlink",
    "changeCondition","changeTime","recordOpeningTime","duration","causeForRecClosing","diagnostics",
    "recordSequenceNumber","nodeID","recordExtensions","localSequenceNumber","apnSelectionMode","servedMSISDN",
    "chargingCharacteristics","chChSelectionMode","iMSsignalingContext","externalChargingID",
    "servingNodePLMNIdentifier","pSFurnishChargingInformation","servedIMEISV","rATType","mSTimeZone",
    "userLocationInformation","cAMELChargingInformation","listOfServiceData","servingNodeType","servedMNNAI",
    "p-GWPLMNIdentifier","startTime","stopTime","served3gpp2MEID","pDNConnectionID","gpp2UserLocationInformation",
    "servedPDPPDNAddressExt","FileSourceName"
]

def load_headers() -> Dict[str, List[str]]:
    """
    Retourne les en-têtes exacts, sans auto-inférence.
    - OCC -> fichiers MGGOCC (.ber, .ber_NP, .ber-SMS)
    - CCN -> fichiers CCNCDR*
    - SGSN -> fichiers MGAOCC* (PGW) '|' délimités
    """
    return {
        "OCC": OCC_HEADERS,
        "CCN": CCN_HEADERS,
        "SGSN": SGSN_HEADERS,
    }
