import json
from datetime import datetime
from typing import List, Optional, Union

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter()


# Modèle Pydantic pour la réponse des visiteurs
class RetailDataResponse(BaseModel):
    store_id: str
    store_name: str
    date: str
    hour: int
    visitors: Optional[int]
    sales: Optional[int]


# Charger les données des visiteurs depuis le fichier JSON retail_data
def load_retail_data():
    """
    Charge les données de retail depuis le fichier JSON 'retail_data.json'.

    Returns:
        list: Liste des données retail chargées depuis le fichier.
        []: Si le fichier est introuvable ou vide.

    Raises:
        FileNotFoundError: Si le fichier 'retail_data.json' est introuvable.
    """
    try:
        with open("data_api/retail_data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print("Le fichier des données de retail n'existe pas.")
        return []


@router.get("", response_model=List[RetailDataResponse])
async def get_visitors(date: str):
    """
    Route GET pour récupérer les données retail d'une date spécifique.

    Args:
        date (str): La date pour laquelle récupérer les données, au format 'YYYY-MM-DD'.

    Returns:
        List[RetailDataResponse]: Liste des données retail pour la date donnée.
        JSONResponse: Erreur si la date est invalide ou si les données ne sont pas disponibles.
    """
    # Vérifier si la date est valide
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(
            content={"error": "Date format is incorrect. Use 'YYYY-MM-DD'."},
            status_code=400,  # Code de statut HTTP 400 (mauvaise requête)
        )
    # Charger les données de retail
    try:
        retail_data = load_retail_data()  # Cette fonction charge les données du fichier
    except FileNotFoundError:
        return JSONResponse(
            content={"error": "Retail data file not found."},
            status_code=404,
        )

    # Filtrer les données pour inclure uniquement celles correspondant à la date
    response = [
        RetailDataResponse(
            store_id=entry["store_id"],
            store_name=entry["store_name"],
            date=entry["date"],
            hour=entry["hour"],
            visitors=entry["visitors"],
            sales=entry["sales"],
        )
        for entry in retail_data
        if entry["date"] == date
    ]

    return response


@router.get("/store", response_model=List[RetailDataResponse])
async def get_store_visitors(date: str, store_id: str):
    """
    Route GET pour récupérer les données retail d'un magasin spécifique à une date donnée.

    Args:
        date (str): La date pour laquelle récupérer les données, au format 'YYYY-MM-DD'.
        store_id (str): L'identifiant du magasin.

    Returns:
        List[RetailDataResponse]: Liste des données retail pour le magasin et la date donnés.
        JSONResponse: Erreur si la date est invalide ou si les données ne sont pas disponibles.
    """
    # Vérifier si la date est valide
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {"error": "Date format is incorrect. Use 'YYYY-MM-DD'."}

    # Charger les données de retail
    try:
        retail_data = load_retail_data()  # Cette fonction charge les données du fichier
    except FileNotFoundError:
        print("Le fichier des données de retail n'existe pas.")
        return {"error": "Retail data file not found."}

    # Filtrer les données pour inclure uniquement celles correspondant à la date et au store_id
    response = [
        RetailDataResponse(
            store_id=entry["store_id"],
            store_name=entry["store_name"],
            date=entry["date"],
            hour=entry["hour"],
            visitors=entry["visitors"],
            sales=entry["sales"],
        )
        for entry in retail_data
        if entry["date"] == date and entry["store_id"] == store_id
    ]

    return response
