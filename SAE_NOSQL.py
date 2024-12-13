# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 13:42:36 2024

@author: mathurin.bernonville
"""

# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexion
conn = sqlite3.connect("Z:/BUT3/SAE_No-SQL/ClassicModel.sqlite")

# Récupération du contenu de Customers avec une requête SQL
customers = pandas.read_sql_query("SELECT * FROM Customers;", conn)
print(customers)

###Question 1
clients_sans_commande = pandas.read_sql_query(
    """SELECT Customers.customerNumber, Customers.customerName 
    FROM Customers 
    LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber 
    WHERE Orders.orderNumber IS NULL;""", conn)
    
print("Clients sans commande:")
print(clients_sans_commande)

###Question 2
stats_par_employe = pandas.read_sql_query (
                """SELECT Employees.employeeNumber, Employees.lastName, Employees.firstName, 
                   COUNT(DISTINCT Customers.customerNumber) AS numberOfClients, 
                   COUNT(DISTINCT Orders.orderNumber) AS numberOfOrders, 
                   SUM(Payments.amount) AS totalPaidAmount
            FROM Employees
            LEFT JOIN Customers ON Employees.employeeNumber = Customers.salesRepEmployeeNumber
            LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
            LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
            GROUP BY Employees.employeeNumber;
       """, conn)
       
print("Statistiques par employé:")
print(stats_par_employe)

###Question 3
stats_par_bureau = pandas.read_sql_query(""" 
                SELECT Offices.officeCode, Offices.city, 
                       COUNT(DISTINCT Customers.customerNumber) AS numberOfClients,
                       COUNT(DISTINCT Orders.orderNumber) AS numberOfOrders, 
                       SUM(OrderDetails.quantityOrdered * OrderDetails.priceEach) AS totalOrderAmount,
                       COUNT(DISTINCT CASE WHEN Customers.country != Offices.country THEN Customers.customerNumber END) AS foreignCustomers
                FROM Offices
                LEFT JOIN Employees ON Offices.officeCode = Employees.officeCode
                LEFT JOIN Customers ON Employees.employeeNumber = Customers.salesRepEmployeeNumber
                LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
                LEFT JOIN OrderDetails ON Orders.orderNumber = OrderDetails.orderNumber
                GROUP BY Offices.officeCode;""", conn)

print("Statistiques par bureau:")
print(stats_par_bureau)


###Question 4

produits_stats = pandas.read_sql_query("""
        SELECT Products.productCode, Products.productName, 
               COUNT(DISTINCT Orders.orderNumber) AS numberOfOrders, 
               SUM(OrderDetails.quantityOrdered) AS totalQuantityOrdered, 
               COUNT(DISTINCT Orders.customerNumber) AS numberOfUniqueCustomers
        FROM Products
        LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
        LEFT JOIN Orders ON OrderDetails.orderNumber = Orders.orderNumber
        GROUP BY Products.productCode;
        """, conn)
print("Statistiques par produit:")
print(produits_stats)

###Question 5
pays_stats = pandas.read_sql_query("""
    SELECT Customers.country, 
           COUNT(DISTINCT Orders.orderNumber) AS numberOfOrders, 
           SUM(OrderDetails.quantityOrdered * OrderDetails.priceEach) AS totalOrderAmount, 
           SUM(Payments.amount) AS totalPaidAmount
    FROM Customers
    LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
    LEFT JOIN OrderDetails ON Orders.orderNumber = OrderDetails.orderNumber
    LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
    GROUP BY Customers.country;
    """, conn)
print("Statistiques par pays:")
print(pays_stats)

###Question 6

pays_produits = pandas.read_sql_query("""
    SELECT Products.productLine, Customers.country, 
           COUNT(DISTINCT Orders.orderNumber) AS numberOfOrders
    FROM Products
    LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
    LEFT JOIN Orders ON OrderDetails.orderNumber = Orders.orderNumber
    LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
    GROUP BY Products.productLine, Customers.country;
    """, conn)
print("Statistiques par pays et par produits:")
print(pays_produits)


###Question 7

details_paiements = pandas.read_sql_query("""
    SELECT Products.productLine, Customers.country, 
           SUM(Payments.amount) AS totalPaidAmount
    FROM Products
    LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
    LEFT JOIN Orders ON OrderDetails.orderNumber = Orders.orderNumber
    LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
    LEFT JOIN Payments ON Orders.customerNumber = Payments.customerNumber
    GROUP BY Products.productLine, Customers.country;
    """, conn)
print("Détail des paiements:")
print(details_paiements)

###Question 8

q8 = pandas.read_sql_query("""
    SELECT Products.productCode, Products.productName, 
       AVG(OrderDetails.priceEach - Products.buyPrice) AS averageMargin
FROM Products
LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
GROUP BY Products.productCode
ORDER BY averageMargin DESC
LIMIT 10;
    """, conn)
print("Top 10:")
print(q8)

###Question 9

q9 = pandas.read_sql_query("""
                   SELECT Products.productCode, Products.productName, 
                       Customers.customerNumber, Customers.customerName, 
                       OrderDetails.priceEach, Products.buyPrice
                FROM Products
                LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
                LEFT JOIN Orders ON OrderDetails.orderNumber = Orders.orderNumber
                LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
                WHERE OrderDetails.priceEach < Products.buyPrice;
    """, conn)
print("Produits vendus à perte:")
print(q9)

# Fermeture de la connexion : IMPORTANT à faire dans un cadre professionnel
conn.close()



####Réflexion sur le format des données pour NoSQL
Dans une base de données NoSQL, les données sont généralement dénormalisées pour minimiser les jointures. Les structures de documents (comme dans MongoDB) peuvent être utilisées pour imbriquer des collections. Par exemple :

Une collection Customers pourrait contenir les informations des clients, avec les commandes imbriquées comme sous-documents.
Les Offices pourraient contenir des sous-documents relatifs aux employés et leurs clients respectifs.
