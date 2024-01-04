
# -*- coding: utf-8 -*-

import psycopg2
import time
from time import sleep
from tqdm import tqdm


#fun auxilar  

def remove_repetidos(lista): # remove valores repetidos
    l = []
    for i in lista:
        if i not in l:
            l.append(i)
    l.sort()
    return l

def limpandoVetores(vetor):
    vetor = vetor.replace('\r', "")
    vetor= vetor.split("\n")
    vetor = filter(None, vetor)
    vetor.append("end")
    vetor = ', '.join(vetor)
    return vetor

def criando_sublistas(lista_grande, tam_sublistas): #peguei da net pra me ajudar e foi uma mãozona <3
    qtd_elementos = len(lista_grande)
    lista_de_lista = []
    for i in range(0, qtd_elementos,  tam_sublistas):        
        # Create an index range for l of n items:
        lista_de_lista.append(lista_grande[i:i+tam_sublistas])
    
    return lista_de_lista


def extrairArq():
    file = open("amazon-meta.txt", 'r')
    file.seek(82) # tira o cabeçario 
    valores = file.read() # bota tudo numa string

    valores = valores.split("\nId") # separa tudo atraves do id
    produtos = []
    for i, valor in enumerate(valores):
        if(i<6000):
            valor = limpandoVetores(valor)
            
            id = valor[valor.find(":   ")+4:valor.find("ASIN")].replace('\r\n', "").strip(" ")
            id = id.replace(",", "")
            
            if "discontinued product" in valor:
                asin = valor[valor.find("ASIN: ")+7:valor.find("discontinued product")].replace('\r\n', "").strip(" ")
                produtos.append((id, asin, "discontinued product"))
                #print(id, asin, "discontinued product") 
            else:
                asin = valor[valor.find("ASIN: ")+7:valor.find("title: ")].replace('\r\n', "").strip(" ")
                title = valor[valor.find("title: ")+7:valor.find("group")].replace('\r\n', "").strip(" ")
                group = valor[valor.find("group: ")+7:valor.find("salesrank")].replace('\r\n', "").strip(" ")
                salesrank = valor[valor.find("salesrank: ")+11:valor.find("similar")].replace('\r\n', "").strip(" ")
                
                similar = valor[valor.find("similar: ")+9:valor.find("categories")].replace('\r\n', "").strip(" ")
                categories = valor[valor.find("categories: ")+12:valor.find("reviews")].replace('\r\n', "").strip(" ")
                reviews = valor[valor.find("reviews: ")+16:valor.find("end")].replace('\r\n', "").strip(" ")
                produtos.append((id,asin, title, group, salesrank, similar, categories, reviews))
                #print(id,asin, title, group, salesrank, similar, categories, reviews) 
                    
                #print(produtos)
    file.close()
               
    return produtos #retorna uma lista de produtos com todos os atributos dele
                

def conexaoBD(): # Fazendo conexao com o postgres
    con = psycopg2.connect(host='localhost', database='database', user='tp1', password='123')
    return con



def inserirBD(sql):
    con = conexaoBD()
    cur = con.cursor()
    try:
        cur.execute(sql)
        #cur.executemany(sql, data)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()
 
 
def inserindoProduct(id, asin, title, group, salesrank, qtd_similares, cat, review):
    if review != []:
        review_total = int(review[0])
        avg_rating = float(review[5].replace(",", ""))
        #print(id, asin, title, group, salesrank, qtd_similares, cat, review_total, avg_rating)
        inserirBD(''' INSERT into public.product (
                    Product_Id , 
                    Product_IdAsin, 
                    Product_Title,
                    Product_Group,
                    Product_SalesRank,
                    Qtd_Similiar,
                    Product_Categories,
                    Total_Review,
                    Avg_Rating) values('%d','%s', '%s', '%s', '%d', '%d','%d','%d','%f');''' % 
                    (id, asin, title, group, salesrank, qtd_similares, cat, review_total, avg_rating))
        
    elif review == []: # se não tem review para a tabela produto
        #print(id, asin, title, group, salesrank, qtd_similares, cat)
        inserirBD(''' INSERT into public.product (
                    Product_Id , 
                    Product_IdAsin, 
                    Product_Title,
                    Product_Group,
                    Product_SalesRank,
                    Qtd_Similiar,
                    Product_Categories
                    ) values('%d','%s', '%s', '%s', '%d', '%d','%d');''' % 
                    (id, asin, title, group, salesrank, qtd_similares, cat))
            
    
 
def inserindoReview(id, reviews):
    aux = []
    if reviews != []: # verificar lista vazia nos reviews
        review = reviews[6:] #pega a partir do primeiro review
        review = criando_sublistas(review, 9) # quebra uma lista nao vazia para separar os reviews de um produto
        #print(review)
        for i in range(len(review)): #passa por todos os reviews do produto e poe no banco
            aux = review[i]
            date = aux[0]
            custumer = aux[2]
            rating = int(aux[4])
            vote = int(aux[6])
            helpful = int(aux[8].replace(",", ""))
            #print(id, date, custumer,rating, vote, helpful)
            inserirBD(''' INSERT into public.Review( 
                    Product_Id,
                    Review_Id,
                    Date , 
                    Customer, 
                    Rating, 
                    Vote, 
                    Helpful)  values('%d',DEFAULT,'%s','%s','%d','%d','%d');''' % 
                    (id, date, custumer,rating, vote, helpful))
         
        
        

def inserindoCategorias(id,categorias):
    if categorias != []:
        categorias = categorias[1:]
        categorias.append("|")
        categorias = ', '.join(categorias)
        categorias = categorias.replace(",", "")
        categorias = categorias.split("|")
        categorias = filter(None, categorias)
        categorias = remove_repetidos(categorias)
        for i in range(len(categorias)):
            cat = categorias[i]
            #cat_id = int(cat[cat.find("[")+1:cat.find("]")])
            cat_id = cat[cat.find("[")+1:cat.find("]")]
            if cat_id == 'guitar':
                cat_id = 0
            else:
                cat_id = int(cat[cat.find("[")+1:cat.find("]")])
            cat_nome = cat[:cat.find("[")]
            cat_nome = cat_nome.replace("'", "")
            #print(id,cat_nome, cat_id)
            inserirBD(''' INSERT into public.CategoryProduct( 
                              Product_Id, 
                              Category_Name,  
                              Category_Id, 
                              Cp_Id)  values('%d','%s','%d',DEFAULT);''' % (id,cat_nome, cat_id))
    
  
def inserindoSimilares(id, similares):
    if similares  != []:
        similar = similares[0].replace(",", "")
        for j in range (1, int(similar)+1):
            similar = similares[j].replace(",", "")
            #print(id, similar)
            inserirBD(''' INSERT into public.SimilarProduct(
                                Product_Id, 
                                Similar_IdAsin,
                                Sp_Id) values('%d','%s',DEFAULT);''' % 
                    (id, similar))
            



   
def povoandoTabelas(produtos):
    for i in tqdm(range(len(produtos))): # percorre todos os produtos
        produto = produtos[i] # pega um produto
        id = int(produto[0]) # pega o id do produto
        asin = produto[1].replace(",", "") # pega o asin do produto
        
        if(produto[2] == "discontinued product"): # verifica se ele é um produto descontinuado
            #print(id, asin) # colocar apenas o id e o asin na tabela product
            inserirBD(''' INSERT into public.product (
                Product_Id , 
                Product_IdAsin) values('%d','%s');''' % 
                (id, asin))
            
        if(produto[2] != "discontinued product"): # se nao for um produto descontinuado 
            title = produto[2].replace(",", "")
            title = title.replace("'", "")
            
            group = produto[3].replace(",", "")
            salesrank = produto[4].replace(",", "")
            
            salesrank = int(salesrank)
            similares = produtos[i][5].split(" ")
            
            qtd_similares = filter(None, similares)
            qtd_similares = int(qtd_similares[0].replace(",", ""))
            
            categorias = produtos[i][6].split(" ")
            categorias = filter(None, categorias)
            cat = int(categorias[0].replace(",", ""))
            
            reviews = produtos[i][7].split(" ")
            reviews = filter(None, reviews)
            
            inserindoProduct(id, asin, title, group, salesrank, qtd_similares, cat, reviews) #ok por enquanto
            inserindoReview(id, reviews) # demora mais
            inserindoCategorias(id, categorias)
            inserindoSimilares(id,similares)
            

    
            

        

#Criando a tabela Product
tableProduct = '''create table if not exists Product (
                Product_Id  int primary key not null, 
                Product_IdAsin varchar(1000)  not null, 
                Product_Title varchar(1000), 
                Product_SalesRank int, 
                Product_Group  varchar(1000),
                Qtd_Similiar int,
                Product_Categories int,
                Total_Review int,
                Avg_Rating float);'''


#Criando a tabela Review 
tableReview = '''create table if not exists Review (
                Review_Id SERIAL primary key not null, 
                Product_Id int not null, 
                Date varchar(100) not null, 
                Customer varchar(30), 
                Rating int, 
                Vote int, 
                Helpful int, 
                foreign key(Product_Id) REFERENCES Product(Product_Id));'''


#Criando a tabela CategoryProduct 
tableCategoryProduct = '''create table if not exists CategoryProduct( 
                        Product_Id int not null, 
                        Category_Name varchar(50), 
                        Category_Id int,
                        Cp_Id SERIAL primary key not null,
                        foreign key(Product_Id) REFERENCES Product(Product_Id))'''

# Criando a tabela SimimarProduct ####################
tableSimilarProduct = '''create table if not exists  SimilarProduct( 
                        Product_Id int not null, 
                        Similar_IdAsin varchar(100), 
                        Sp_Id SERIAL primary key not null,
                        foreign key(Product_Id) REFERENCES Product(Product_Id))'''
    
    
    
    
        
                    
def tempo(t):
    tm = ''
    if(t >= 60):
        t = t/60
        tm = str(round(t)) + ' ' + 'minutos' 
    else:
        tm = str(round(t, 2)) + ' '+ 'segundos'
              
    return tm

if __name__ == "__main__":
    
    print("Deletando a tabela product...")
    sql = 'DROP TABLE IF EXISTS public.Product CASCADE'
    ini = time.time()
    inserirBD(sql)
    fim = time.time()
    t = fim-ini
    print('Tabela product deletada em '+ tempo(t))
    
    print("Criando a tabela product...")
    ini = time.time()
    inserirBD(tableProduct)
    fim = time.time()
    t = fim-ini
    print('Tabela product criada em '+ tempo(t))
    
    
    print("Deletando a tabela review...")
    sql = 'DROP TABLE IF EXISTS public.Review'
    ini = time.time()
    inserirBD(sql)
    fim = time.time()
    t = fim-ini
    print('Tabela review deletada em '+ tempo(t))
    
    
    print("Criando a tabela review...")
    ini = time.time()
    inserirBD(tableReview)
    fim = time.time()
    t = fim-ini
    print('Tabela review criada em '+ tempo(t))
    
    
    print("Deletando a tabela categoryproduct...")
    sql = 'DROP TABLE IF EXISTS public.CategoryProduct'
    ini = time.time()
    inserirBD(sql)
    fim = time.time()
    t = fim-ini
    print('Tabela categoryproduct deletada em '+ tempo(t))
    
    
    print("Criando a tabela categoryproduct...")
    ini = time.time()
    inserirBD(tableCategoryProduct)
    fim = time.time()
    t = fim-ini
    print('Tabela categoryproduct criada em '+ tempo(t))
    
    
    
    print("Deletando a tabela similarproduct...")
    sql = 'DROP TABLE IF EXISTS public.SimilarProduct'
    ini = time.time()
    inserirBD(sql)
    fim = time.time()
    t = fim-ini
    print('Tabela similarproduct deletada em '+ tempo(t))
    
    
    print("Criando a tabela similarproduct...")
    ini = time.time()
    inserirBD(tableSimilarProduct)
    fim = time.time()
    t = fim-ini
    print('Tabela similarproduct criada em '+ tempo(t))
    
    print("Extraindo do arquivo...")
    ini = time.time()
    produtos = extrairArq()
    fim = time.time()
    t = fim-ini
    print('Arquivo extraido em '+ tempo(t))
    #print(produtos)
    
    print("Inserindo valores nas tabelas Product, Review, CategoryProduct, SimilarProduct...")
    ini = time.time()
    povoandoTabelas(produtos)
    fim = time.time()
    t = fim-ini
    print("Tempo total para preencher as tabelas:" +" " + tempo(t))
