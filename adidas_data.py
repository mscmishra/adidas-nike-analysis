import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

df_adidas = pd.read_csv('adidas_data.csv')
data_adidas = pd.DataFrame(df_adidas, columns=['product_name', 'product_id', 'listing_price', 'sale_price', 'discount'])
# print(data_adidas)

df_nike = pd.read_csv('nike_data.csv')
data_nike = pd.DataFrame(df_nike, columns=['Product Name', 'Product ID', 'Listing Price', 'Sale Price', 'Discount'])

con = sqlite3.connect('adidas_nike.sqlite')
cur = con.cursor()

con2 = sqlite3.connect('discount.sqlite')
cur2 = con2.cursor()

cur2.execute('''CREATE TABLE IF NOT EXISTS ad_ni_discount 
            (id  INTEGER NOT NULL PRIMARY KEY UNIQUE, discount TEXT UNIQUE, count_adidas INTEGER, count_nike INTEGER)''')


cur.execute('''CREATE TABLE IF NOT EXISTS adidas 
            (id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, product_name TEXT, product_id TEXT UNIQUE,
             listing_price INTEGER, sale_price INTEGER, discount INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS nike 
            (id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, product_name TEXT, product_id TEXT UNIQUE,
             listing_price INTEGER, sale_price INTEGER, discount INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS ad_ni_price 
            (id  INTEGER NOT NULL PRIMARY KEY UNIQUE, Price_Range TEXT UNIQUE, mrpcount_adidas INTEGER,
                 mrpcount_nike INTEGER, spcount_adidas INTEGER, spcount_nike INTEGER)''')


for row in data_adidas.itertuples():
    # print(row)
    pn = row.product_name
    pi = row.product_id
    lp = row.listing_price
    sp = row.sale_price
    dis = row.discount

    # print(pn, pi, lp, sp, dis)
    cur.execute('''
                INSERT OR IGNORE INTO adidas (product_name, product_id, listing_price, sale_price, discount)
                 VALUES (?, ?, ?, ?, ?)''', (pn, pi, lp, sp, dis))

for row in data_nike.itertuples():
    # print(row)
    pn = row._1
    pi = row._2
    lp = row._3
    sp = row._4
    dis = row.Discount

    # print(pn, pi, lp, sp, dis)
    cur.execute('''
                INSERT OR IGNORE INTO nike (product_name, product_id, listing_price, sale_price, discount)
                 VALUES (?, ?, ?, ?, ?)''', (pn, pi, lp, sp, dis))


cur.execute('''SELECT MIN(listing_price) FROM adidas''')
minrow_ad = cur.fetchone()[0]
cur.execute('''SELECT MAX(listing_price) FROM adidas''')
maxrow_ad = cur.fetchone()[0]
cur.execute('''SELECT COUNT (id) FROM adidas''')
count_adidas = cur.fetchone()[0]
print("Adidas has",count_adidas ,"products, and its price ranges from", minrow_ad, 'to', maxrow_ad)

cur.execute('''SELECT MIN(listing_price) FROM nike''')
minrow_ni = cur.fetchone()[0]
cur.execute('''SELECT MAX(listing_price) FROM nike''')
maxrow_ni = cur.fetchone()[0]
cur.execute('''SELECT COUNT (id) FROM nike''')
count_nike = cur.fetchone()[0]
print("Nike has",count_nike ,"products, and its price ranges from", minrow_ni, 'to', maxrow_ni)

# plt.bar(data_adidas['listing_price'], data_nike['Listing Price'])
# plt.xlabel("Nike Listing Price")
# plt.ylabel("Adidas Listing Price")
# plt.show()

def column_range(brand_name, column_name, LOWER, UPPER):
    range_list = []
    cur.execute('''SELECT product_id FROM {} WHERE {} BETWEEN {} AND {}'''.format(brand_name, column_name, LOWER, UPPER))
    w = cur.fetchall()
    # print(w)

    range_list= map(lambda row: row[0], w)
    return list(range_list)
    # for rows in w:
    #     range_list.append(rows[0])

    # return range_list


id = 1
range = 2000
start_range = 0
end_range = range
cur.execute('''DELETE FROM ad_ni_price''')

if maxrow_ni > maxrow_ad:
    while end_range < maxrow_ni+range:
        my_str = str(start_range) + ' - ' + str(end_range)
        count_list = column_range('adidas', 'listing_price', start_range, end_range)
        count_list2 = column_range('nike', 'listing_price', start_range, end_range)
        count_list3 = column_range('adidas', 'sale_price', start_range, end_range)
        count_list4 = column_range('nike', 'sale_price', start_range, end_range)

        count = len(count_list)
        count2 = len(count_list2)
        count3 = len(count_list3)
        count4 = len(count_list4)

        if count < 1:
            count = 0
        if count2 < 1:
            count2 = 0
        if count3 < 1:
            count3 = 0
        if count4 < 1:
            count4 = 0
        cur.execute('''INSERT OR IGNORE INTO ad_ni_price (id, Price_Range, mrpcount_adidas, mrpcount_nike, spcount_adidas, spcount_nike)
                 VALUES (?, ?, ?, ?, ?, ?)''', (id, my_str,  count, count2, count3, count4))
        start_range = end_range
        end_range = end_range + range
        id = id + 1
else:
    print("maxrow_ad > maxrow_ni. Change the code")


cur.execute('''SELECT COUNT (id) FROM ad_ni_price''')
count_rows = cur.fetchone()[0]
print(count_rows)

id_dis = 1
range_dis = 10
lower_dis = 0
higher_dis = range_dis
cur2.execute('''DELETE FROM ad_ni_discount''')

if maxrow_ni > maxrow_ad:
    while higher_dis < 101:
        my_str2 = str(lower_dis) + ' - ' + str(higher_dis)
        count_list = column_range('adidas', 'discount', lower_dis, higher_dis)
        count_list2 = column_range('nike', 'discount', lower_dis, higher_dis)
        count = len(count_list)
        count2 = len(count_list2)
        if count < 1:
            count = 0
        if count2 < 1:
            count2 = 0
        cur2.execute('''INSERT OR IGNORE INTO ad_ni_discount (id, discount, count_adidas, count_nike)
                    VALUES (?, ?, ?, ?)''', (id_dis, my_str2, count, count2))
        lower_dis = higher_dis
        higher_dis = higher_dis + range_dis
        id_dis = id_dis + 1
else:
    print("maxrow_ad > maxrow_ni. Change the code")

cur2.execute('''SELECT COUNT (id) FROM ad_ni_discount''')
rows = cur2.fetchone()[0]
print(rows)

con.commit()
con2.commit()