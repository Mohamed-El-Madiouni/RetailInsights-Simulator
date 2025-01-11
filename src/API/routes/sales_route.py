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
    """
    Charge les ventes depuis le fichier JSON 'sales.json'.

    Returns:
        list: Liste des ventes chargées depuis le fichier.
        []: Si le fichier n'existe pas ou est vide.

    Raises:
        FileNotFoundError: Si le fichier 'sales.json' est introuvable.
    """
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
    Route GET pour récupérer les ventes d'un magasin à une date donnée.

    Args:
        sale_date (str): La date des ventes, au format 'YYYY-MM-DD'.
        store_id (str): L'identifiant du magasin.

    Returns:
        List[SaleResponse]: Liste des ventes filtrées pour la date et le magasin donnés.
        dict: Message d'erreur si aucune vente n'est trouvée ou si le fichier est introuvable.
    """
    # Charger les données des ventes
    try:
        sales = load_sales()
    except FileNotFoundError:
        return {"erreur": "Le fichier sales n'existe pas."}

    # Filtrer les ventes pour inclure uniquement celles correspondant aux critères spécifiés
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
    Route GET pour récupérer les ventes à une date et une heure donnée.

    Args:
        sale_date (str): La date des ventes, au format 'YYYY-MM-DD'.
        hour (str): L'heure des ventes, au format 'HH'.

    Returns:
        List[SaleResponse]: Liste des ventes pour la date et l'heure spécifiées.
        dict: Message d'erreur si aucune vente n'est trouvée ou si le fichier est introuvable.
    """
    # Charger les données des ventes
    try:
        sales = load_sales()
    except FileNotFoundError:
        return {"erreur": "Le fichier sales n'existe pas."}

    # Filtrer les ventes pour inclure uniquement celles correspondant aux critères spécifiés
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
