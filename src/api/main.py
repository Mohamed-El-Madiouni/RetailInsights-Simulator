"""
Fichier principal pour configurer et démarrer l'api Retail Insights.

Ce fichier inclut les routeurs définis dans différents modules et lance le serveur.
"""

from fastapi import FastAPI

from src.api.routes.clients_route import router as client_router
from src.api.routes.products_route import router as product_router
from src.api.routes.retail_data_route import router as retail_data_router
from src.api.routes.sales_route import router as sales_router
from src.api.routes.stores_route import router as store_router

app = FastAPI()


# Routeur pour les ventes
app.include_router(sales_router, prefix="/sales", tags=["Sales"])
# Routeur pour les clients
app.include_router(client_router, prefix="/clients", tags=["Clients"])
# Routeur pour les products
app.include_router(product_router, prefix="/products", tags=["Products"])
# Routeur pour les retail_data
app.include_router(retail_data_router, prefix="/retail_data", tags=["RetailData"])
# Routeur pour les stores
app.include_router(store_router, prefix="/stores", tags=["Stores"])


@app.get("/")
def get_welcome():
    """
    Point de terminaison d'accueil de l'api.

    Returns:
        dict: Message de bienvenue.
    """
    return {"message": "Bienvenue sur l'api Retail Insights!!"}


# Lancer le serveur api localement pour tester
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
