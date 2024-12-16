from fastapi import FastAPI

from src.routes.sales_route import router as sales_router
from src.routes.clients_route import router as client_router
from src.routes.products_route import router as product_router
from src.routes.retail_data_route import router as retail_data_router
from src.routes.stores_route import router as store_router

app = FastAPI()


app.include_router(sales_router, prefix="/sales", tags=["Sales"])
app.include_router(client_router, prefix="/clients", tags=["Clients"])
app.include_router(product_router, prefix="/products", tags=["Products"])
app.include_router(retail_data_router, prefix="/retail_data", tags=["RetailData"])
app.include_router(store_router, prefix="/stores", tags=["Stores"])


@app.get("/")
def get_welcome():
    return {"message": "Bienvenue sur l'API Retail Insights!!"}


# Exemple d'utilisation pour tester
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='localhost', port=8000)
