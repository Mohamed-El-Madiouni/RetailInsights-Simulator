import json
from typing import List

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


# Modèle Pydantic pour la réponse des ventes
class SaleResponse(BaseModel):
    sale_id: str
    nb_type_product: int
    product_id: str
    client_id: str
    store_id: str
    quantity: int
    sale_amount: float
    sale_date: str
    sale_time: str


# Charger les données des ventes depuis le fichier JSON
def load_sales():
    """Charge les ventes depuis le fichier JSON 'sales.json'."""
    try:
        with open("data_api/sales.json", "r", encoding="utf-8") as f:
            sales = json.load(f)
        return sales
    except FileNotFoundError:
        print("Le fichier des ventes n'existe pas.")
        return []


@router.get("", response_model=List[SaleResponse])
async def get_sales(sale_date: str, store_id: str):
    """
    Route GET pour récupérer la liste des ventes dans un magasin à une date donnée.
    Les arguments 'sale_date' et 'store_id' sont requis pour filtrer les ventes.
    """
    # Charger les données des ventes
    try:
        sales = load_sales()
    except FileNotFoundError:
        return {"erreur": "Le fichier sales n'existe pas."}

    # Filtrer les ventes en fonction de la date et du store_id
    filtered_sales = [
        sale
        for sale in sales
        if sale["sale_date"] == sale_date and sale["store_id"] == store_id
    ]

    # Si aucune vente n'est trouvée pour la date et le magasin spécifiés
    if not filtered_sales:
        return {"error": f"No sales found for store ID: {store_id} on {sale_date}"}

    # Retourner la liste des ventes filtrées
    return filtered_sales


@router.get("/hour", response_model=List[SaleResponse])
async def get_sales_by_hour(sale_date: str, hour: str):
    """
    Route GET pour récupérer la liste des ventes d'une date et une heure donnée.
    Les arguments 'sale_date' et 'hour' sont requis pour filtrer les ventes.
    """
    # Charger les données des ventes
    try:
        sales = load_sales()
    except FileNotFoundError:
        return {"erreur": "Le fichier sales n'existe pas."}

    # Filtrer les ventes en fonction de la date et l'heure
    filtered_sales = [
        sale
        for sale in sales
        if int(sale["sale_time"][:2]) == int(hour.zfill(2))
        and sale["sale_date"] == sale_date
    ]

    # Si aucune vente n'est trouvée pour la date et l'heure spécifiés
    if not filtered_sales:
        return {"error": f"No sales found for hour: {hour} on {sale_date}"}

    # Retourner la liste des ventes filtrées
    return filtered_sales
