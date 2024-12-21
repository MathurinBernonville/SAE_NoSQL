# Installer la bibliothèque pymongo pour interagir avec MongoDB
# pip install pymongo

import sqlite3  # Importation de sqlite3 pour la gestion des bases de données SQLite
import pandas  # Importation de pandas pour la manipulation des données sous forme de DataFrame
import pymongo  # Importation de pymongo pour la gestion de MongoDB


# Connexion à la base de données MongoDB (en utilisant une URI spécifique)
client = pymongo.MongoClient("mongodb+srv://mathurinbernonville:AGVF2fgFGOP8RX@cluster-but-sd.cgskt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-but-sd") 
db = client.sae  # Sélection de la base de données "sae"

# Connexion à la base de données SQLite locale (ClassicModel.sqlite)
conn = sqlite3.connect("ClassicModel.sqlite")


# Exécution de requêtes SQL pour récupérer les données des différentes tables SQLite
Products = pandas.read_sql_query("""
    SELECT *
    FROM Products;
""", conn)

OrderDetails = pandas.read_sql_query("""
    SELECT *
    FROM OrderDetails;
""", conn)

Orders = pandas.read_sql_query("""
    SELECT *
    FROM Orders;
""", conn)

Customers = pandas.read_sql_query("""
    SELECT *
    FROM Customers;
""", conn)

Employees = pandas.read_sql_query("""
    SELECT *
    FROM Employees;
""", conn)

Offices = pandas.read_sql_query("""
    SELECT *
    FROM Offices;
""", conn)

Payments = pandas.read_sql_query("""
    SELECT *
    FROM Payments;
""", conn)

# Affichage du DataFrame Customers
Customers


#### Fusionner les données de la table Employees avec celles de la table Offices
offices = [
    Offices.query('officeCode == @id')
        .drop(columns=["officeCode"])  # Retirer la colonne officeCode après la jointure
        .to_dict(orient = "records")   # Conversion en dictionnaire pour MongoDB
    for id in Employees.officeCode  # Pour chaque code d'office dans la table Employees
]

print(offices)

# Fusion des données d'Employees avec celles des bureaux (Offices)
Employees_offices = Employees.assign(Offices = offices)
Employees_offices.head()  # Affichage des 5 premières lignes du DataFrame résultant


#### Fusionner Products et OrderDetails pour enrichir la table OrderDetails
products = [
    Products.query('productCode == @id')
        .drop(columns=["productCode"])  # Retirer la colonne productCode après la jointure
        .to_dict(orient = "records")   # Conversion en dictionnaire pour MongoDB
    for id in OrderDetails.productCode  # Pour chaque productCode dans la table OrderDetails
]

print(products)

# Fusionner les données de OrderDetails avec celles de Products
details_product = OrderDetails.assign(Products = products)
details_product.head()  # Affichage des 5 premières lignes du DataFrame résultant


#### Fusionner details_product et Orders
orders = [
    details_product.query('orderNumber == @id')
        .drop(columns=["orderNumber"])  # Retirer la colonne orderNumber après la jointure
        .to_dict(orient = "records")  # Conversion en dictionnaire pour MongoDB
    for id in Orders.orderNumber  # Pour chaque orderNumber dans la table Orders
]

print(orders)

# Fusionner Orders avec les données enrichies de OrderDetails
Orders_product = Orders.assign(OrderDetails_product = orders)
Orders_product.head()  # Affichage des 5 premières lignes du DataFrame résultant


#### Exporter les données vers MongoDB

# Exportation de la table Employees_offices vers MongoDB
db.Employees_offices.insert_many(
    Employees_offices.to_dict(orient = "records")  # Conversion du DataFrame en format adapté pour MongoDB
)

# Exportation de la table details_product vers MongoDB
db.details_product.insert_many(
    details_product.to_dict(orient = "records")
)

# Exportation de la table Customers vers MongoDB
db.Customers.insert_many(
    Customers.to_dict(orient = "records")
)

# Exportation de la table Payments vers MongoDB
db.Payments.insert_many(
    Payments.to_dict(orient = "records")
)

-------------------VERIFICATION AVEC REQUETES NOSQL--------------------------------------------------
# 1. Clients sans commande
clients_sans_commande = db["Customers"].aggregate([
    {
        "$lookup": {
            "from": "Orders",
            "localField": "customerNumber",
            "foreignField": "customerNumber",
            "as": "orders"
        }
    },
    {
        "$match": {"orders": {"$size": 0}}
    },
    {
        "$project": {
            "_id": 0,
            "customerNumber": 1,
            "customerName": 1
        }
    }
])

# 2. Statistiques par employé
stats_par_employe = db["Employees"].aggregate([
    {
        "$lookup": {
            "from": "Customers",
            "localField": "employeeNumber",
            "foreignField": "salesRepEmployeeNumber",
            "as": "customers"
        }
    },
    {
        "$lookup": {
            "from": "Orders",
            "localField": "customers.customerNumber",
            "foreignField": "customerNumber",
            "as": "orders"
        }
    },
    {
        "$lookup": {
            "from": "Payments",
            "localField": "customers.customerNumber",
            "foreignField": "customerNumber",
            "as": "payments"
        }
    },
    {
        "$project": {
            "_id": 0,
            "employeeNumber": 1,
            "lastName": 1,
            "firstName": 1,
            "numberOfClients": {"$size": "$customers"},
            "numberOfOrders": {"$size": "$orders"},
            "totalPaidAmount": {"$sum": "$payments.amount"}
        }
    }
])

# 3. Statistiques par bureau
stats_par_bureau = db["Offices"].aggregate([
    {
        "$lookup": {
            "from": "Employees",
            "localField": "officeCode",
            "foreignField": "officeCode",
            "as": "employees"
        }
    },
    {
        "$lookup": {
            "from": "Customers",
            "localField": "employees.employeeNumber",
            "foreignField": "salesRepEmployeeNumber",
            "as": "customers"
        }
    },
    {
        "$lookup": {
            "from": "Orders",
            "localField": "customers.customerNumber",
            "foreignField": "customerNumber",
            "as": "orders"
        }
    },
    {
        "$lookup": {
            "from": "OrderDetails",
            "localField": "orders.orderNumber",
            "foreignField": "orderNumber",
            "as": "orderDetails"
        }
    },
    {
        "$project": {
            "_id": 0,
            "officeCode": 1,
            "city": 1,
            "numberOfClients": {"$size": "$customers"},
            "numberOfOrders": {"$size": "$orders"},
            "totalOrderAmount": {"$sum": {"$multiply": ["$orderDetails.quantityOrdered", "$orderDetails.priceEach"]}},
            "foreignCustomers": {
                "$size": {
                    "$filter": {
                        "input": "$customers",
                        "as": "customer",
                        "cond": {"$ne": ["$$customer.country", "$country"]}
                    }
                }
            }
        }
    }
])

# 4. Statistiques par produit
produits_stats = db["Products"].aggregate([
    {
        "$lookup": {
            "from": "OrderDetails",
            "localField": "productCode",
            "foreignField": "productCode",
            "as": "orderDetails"
        }
    },
    {
        "$lookup": {
            "from": "Orders",
            "localField": "orderDetails.orderNumber",
            "foreignField": "orderNumber",
            "as": "orders"
        }
    },
    {
        "$project": {
            "_id": 0,
            "productCode": 1,
            "productName": 1,
            "numberOfOrders": {"$size": "$orders"},
            "totalQuantityOrdered": {"$sum": "$orderDetails.quantityOrdered"},
            "numberOfUniqueCustomers": {"$size": {"$setUnion": "$orders.customerNumber"}}
        }
    }
])

# 5. Statistiques par pays
pays_stats = db["Customers"].aggregate([
    {
        "$lookup": {
            "from": "Orders",
            "localField": "customerNumber",
            "foreignField": "customerNumber",
            "as": "orders"
        }
    },
    {
        "$lookup": {
            "from": "OrderDetails",
            "localField": "orders.orderNumber",
            "foreignField": "orderNumber",
            "as": "orderDetails"
        }
    },
    {
        "$lookup": {
            "from": "Payments",
            "localField": "customerNumber",
            "foreignField": "customerNumber",
            "as": "payments"
        }
    },
    {
        "$group": {
            "_id": "$country",
            "numberOfOrders": {"$sum": {"$size": "$orders"}},
            "totalOrderAmount": {"$sum": {"$multiply": ["$orderDetails.quantityOrdered", "$orderDetails.priceEach"]}},
            "totalPaidAmount": {"$sum": "$payments.amount"}
        }
    }
])

# 6. Statistiques par pays et produit
pays_produits = db["Products"].aggregate([
    {
        "$lookup": {
            "from": "OrderDetails",
            "localField": "productCode",
            "foreignField": "productCode",
            "as": "orderDetails"
        }
    },
    {
        "$lookup": {
            "from": "Orders",
            "localField": "orderDetails.orderNumber",
            "foreignField": "orderNumber",
            "as": "orders"
        }
    },
    {
        "$lookup": {
            "from": "Customers",
            "localField": "orders.customerNumber",
            "foreignField": "customerNumber",
            "as": "customers"
        }
    },
    {
        "$group": {
            "_id": {"productLine": "$productLine", "country": "$customers.country"},
            "numberOfOrders": {"$sum": 1}
        }
    }
])

# 7. Statistiques par année
stats_par_annee = db["Orders"].aggregate([
    {
        "$group": {
            "_id": {"year": {"$year": "$orderDate"}},
            "numberOfOrders": {"$sum": 1},
            "totalOrderAmount": {"$sum": {"$multiply": ["$orderDetails.quantityOrdered", "$orderDetails.priceEach"]}}
        }
    }
])

# 8. Commandes annulées
commandes_annulees = db["Orders"].aggregate([
    {
        "$match": {"status": "Cancelled"}
    },
    {
        "$project": {
            "_id": 0,
            "orderNumber": 1,
            "status": 1,
            "customerNumber": 1
        }
    }
])

# 9. Produits les plus commandés
produits_les_plus_commandes = db["OrderDetails"].aggregate([
    {
        "$group": {
            "_id": "$productCode",
            "totalQuantity": {"$sum": "$quantityOrdered"}
        }
    },
    {
        "$sort": {"totalQuantity": -1}
    },
    {
        "$limit": 10}
])
