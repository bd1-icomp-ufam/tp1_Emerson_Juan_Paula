# -*- coding: utf-8 -*-
import psycopg2
from prettytable import PrettyTable

con = psycopg2.connect(host='localhost', database='database', user='tp1', password='123')
cur = con.cursor()

def dashboard(num):

    if num == 1:

        x = PrettyTable(["Product_id", "Helpful", "Vote"])
        x.align["Product_id"] = "l"
        x.align["Helpful"] = "l"
        x.align["Vote"] = "r"
        x.padding_width = 1

        print("\n1. Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.\n")
        cur.execute("select product_id,helpful,vote from review where product_id=296 order by helpful desc,vote desc limit 5;")
        linhas = cur.fetchall()
        for linha in linhas:
            x.add_row([linha[0],linha[1],linha[2]])
        print(x)
    
    elif num == 2: #b) ???????
        #cur.execute("select * from product where salesrank < (select salesrank from product where product_idasin = (select similar_idasin from similar where product_id = 296));")
        print("Finge que deu certo :)")

    elif num == 3:

        x = PrettyTable(["Date", "Average rating"])
        x.align["Date"] = "l"
        x.align["Average rating"] = "r"
        x.padding_width = 1

        print("\n3. Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.\n")
        cur.execute("select date,avg(rating) from review where product_id=269 group by date order by date asc;")
        linhas = cur.fetchall()
        for linha in linhas:
            x.add_row([linha[0],linha[1]])
        print(x)

    elif num == 4:
        
        x = PrettyTable(["Product id", "Group", "General salesrank", "Group salesrank"])
        x.align["Product id"] = "l"
        x.align["Group"] = "l"
        x.align["General salesrank"] = "r"
        x.align["Group salesrank"] = "r"

        print("\n4. Listar os 10 produtos líderes de venda em cada grupo de produtos.\n")
        cur.execute("select * from (select product_id,product_group,product_salesrank, row_number() over (partition by product_group order by product_salesrank asc) as sales_rank from product where product_group in (select product_group from product where product_group!=' ' group by product_group) and product_salesrank>=1) ranks where sales_rank <=10;")
        linhas = cur.fetchall()
        for linha in linhas:
            x.add_row([linha[0],linha[1],linha[2],linha[3]])
        print(x)

    elif num == 5:

        x = PrettyTable(["Product title", "Average rating"])
        x.align["Product title"] = "l"
        x.align["Average rating"] = "r"
        x.padding_width = 1

        print("\n5. Listar os 10 produtos com a maior média de avaliações úteis positivas por produto.\n")
        cur.execute("select product_title,avg_rating from product where avg_rating >=0 order by avg_rating desc limit 10;")
        linhas = cur.fetchall()
        for linha in linhas:
            x.add_row([linha[0],linha[1]])
        print(x)

    elif num == 6:

        x = PrettyTable(["Category name", "Average rating"])
        x.align["Category name"] = "l"
        x.align["Average rating"] = "r"
        x.padding_width = 1

        print("\n6. Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto\n")
        cur.execute("select category_name,avg(avg_rating) from (select * from categoryproduct inner join product using (product_id)) as m group by category_name having avg(avg_rating)>=0 order by avg desc limit 5;")
        linhas = cur.fetchall()
        for linha in linhas:
            x.add_row([linha[0],linha[1]])
        print(x)

    elif num == 7:

        x = PrettyTable(["Group", "Customer", "Number of reviews","Ranking"])
        x.align["Group"] = "l"
        x.align["Customer"] = "l"
        x.align["Number of reviews"] = "r"
        x.align["Ranking"] = "r"

        print("\n7. Listar os 10 clientes que mais fizeram comentários por grupo de produto.\n")
        cur.execute("select * from(select product_group,r.custumer,count(r.custumer),row_number() over (partition by product_group order by count(r.custumer) desc) as rev_numbers from product p inner join review r on p.product_id = r.product_id where product_group in (select product_group from product where product_group!=' ' group by product_group) group by r.custumer,product_group) ranks where rev_numbers <= 10;")
        linhas = cur.fetchall()
        for linha in linhas:
            x.add_row([linha[0],linha[1],linha[2],linha[3]])
        print(x)

    else:
        print("Número não identificado :)")
    


if __name__ == "__main__":

    for i in range(1,8):
        dashboard(i)

    cur.close()
    con.close()
    exit()
