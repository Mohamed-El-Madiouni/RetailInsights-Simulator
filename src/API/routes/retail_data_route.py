from fastapi import APIRouter
from typing import List, Optional
from pydantic import BaseModel
import json
from datetime import datetime


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
    """Charge les données de retail depuis le fichier JSON 'retail_data.json'."""
    try:
        with open('../../data_api/retail_data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print("Le fichier des données de retail n'existe pas.")
        return []


@router.get("", response_model=List[RetailDataResponse])
async def get_visitors(date: str):
    """
    Route GET pour récupérer le nombre total de visiteurs pour un magasin
    à une date donnée.
    La date doit être au format 'YYYY-MM-DD'.
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

    # Filtrer les données pour la date donnée
    response = [
        RetailDataResponse(
            store_id=entry['store_id'],
            store_name=entry['store_name'],
            date=entry['date'],
            hour=entry['hour'],
            visitors=entry['visitors'],
            sales=entry['sales']
        )
        for entry in retail_data if entry['date'] == date
    ]

    return response


@router.get("/store", response_model=List[RetailDataResponse])
async def get_store_visitors(date: str, store_id: str):
    """
    Route GET pour récupérer le nombre total de visiteurs pour un magasin
    à une date donnée.
    La date doit être au format 'YYYY-MM-DD'.
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

    # Filtrer les données pour la date donnée
    response = [
        RetailDataResponse(
            store_id=entry['store_id'],
            store_name=entry['store_name'],
            date=entry['date'],
            hour=entry['hour'],
            visitors=entry['visitors'],
            sales=entry['sales']
        )
        for entry in retail_data if entry['date'] == date and entry['store_id'] == store_id
    ]

    return response
