
import streamlit as st
import pandas as pd
import mysql.connector as mysqlcon
import requests
from streamlit_option_menu import option_menu

connection = mysqlcon.connect(host ='localhost', user= 'root', password = 'Rani', database= 'harvard')
cursor = connection.cursor()

api_key = "d3e1ec2e-fde0-4aca-95ac-45e7bdfcd482"


def object_details(api_key,class_name):
    all_records = []
    url = "https://api.harvardartmuseums.org/object"
    for i in range(1, 26):
        params = {
            "apikey": api_key,
            "size": 100,
            "page": i,
            "classification": class_name
        }
        response = requests.get(url, params=params)
        data = response.json()
        records = data.get('records', [])
        all_records.extend(records)
    return all_records

# Creating tables for metadata,media,colors.....

def create_tables():
        cursor.execute("""CREATE TABLE IF NOT EXISTS artifact_metadata 
               (id INTEGER PRIMARY KEY, 
               title TEXT, 
               culture TEXT, 
               period TEXT, 
               century TEXT, 
               medium TEXT, 
               dimensions TEXT, 
               description TEXT, 
               department TEXT,
               classification TEXT, 
               accessionyear INTEGER, 
               accessionmethod TEXT)""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS artifact_media
               (objectid INT,
               imagecount INT,
               mediacount INTEGER,
               colorcount INTEGER,
               rank_num INTEGER,
               datebegin INTEGER,
               dateend INTEGER,
               foreign key (objectid) references artifact_metadata(id))""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS artifact_colors
               (objectid INTEGER,
               color TEXT,
               spectrum TEXT,
               hue TEXT,
               percent REAL,
               css3 TEXT)""")


# Collecting data for the above tables....

def artifacts_data(records):
    artifact_metadata = []    
    for i in records:
       artifact_metadata.append(dict(id = i['id'],
       title = i['title'],
       culture = i['culture'],
       period = i['period'],
       century = i['century'],
       medium = i['medium'],
       dimensions = i['dimensions'],
       description = i['description'],
       department = i['department'],
       classification = i['classification'],
       aaccessionyear = i['accessionyear'],
       accessionmethod = i['accessionmethod']))
       
    artifact_media = []
    for i in records:
       artifact_media.append(dict(objectid = i['objectid'],
       imagecount = i['imagecount'],
       mediacount = i['mediacount'],
       colorcount = i['colorcount'],
       rank_num = i['rank'],
       datebegin = i['datebegin'],
       dateend = i['dateend']))

    artifact_colors = []
    for i in records:
       if isinstance(i, dict) and 'objectid' in i:
        for j in i.get('colors', []): 
            if isinstance(j, dict): 
                artifact_colors.append({
                    'objectid': i['objectid'],
                    'color': j.get('color'),
                    'spectrum': j.get('spectrum'),
                    'hue': j.get('hue'),
                    'percent': j.get('percent'),
                    'css3': j.get('css3')})

    return artifact_metadata,artifact_media,artifact_colors              

# Inserting the values into the tables.....

def insert_values(artifact_metadata,artifact_media,artifact_colors):
        query = "insert into artifact_metadata (id, title, culture, period, century, medium, dimensions, description, department, classification, accessionyear, accessionmethod) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for i in artifact_metadata:
            values = (i['id'], i['title'], i['culture'], i['period'], i['century'], i['medium'], i['dimensions'], i['description'], i['department'], i['classification'], i['aaccessionyear'], i['accessionmethod'])
            cursor.execute(query, values)

        query = "insert into artifact_media (objectid, imagecount, mediacount, colorcount, rank_num, datebegin, dateend) values (%s, %s, %s, %s, %s, %s, %s)"
        for i in artifact_media:
            values = (i['objectid'], i['imagecount'], i['mediacount'], i['colorcount'], i['rank_num'], i['datebegin'], i['dateend'])
            cursor.execute(query, values)

        query = "insert into artifact_colors (objectid, color, spectrum, hue, percent, css3) values (%s, %s, %s, %s, %s, %s)"
        for i in artifact_colors:
            values = (i['objectid'], i['color'], i['spectrum'], i['hue'], i['percent'], i['css3'])
            cursor.execute(query, values)

        #     return artifact_metadata,artifact_media,artifact_colors

        connection.commit()

# Streamlit visualization.....
        
st.title("🏛️ Harvard's Artifacts Collection: ETL, SQL Analytics & Streamlit Showcase")
st.divider()


classification = st.text_input("Enter a classification:")
button = st.button("collect data")
heading = option_menu(None,['Display the data', 'Migrate to SQL', 'SQL queries'], orientation = 'horizontal')
create_tables()

if button:
        if classification != '':
             records = object_details(api_key, classification)
             artifact_metadata,artifact_media,artifact_colors = artifacts_data(records)
             col1,col2,col3 = st.columns(3)
             with col1:
                     st.header("Metadata")
                     st.json(artifact_metadata)
             with col2:
                     st.header("Media")
                     st.json(artifact_media)
             with col3:
                     st.header("Colors")
                     st.json(artifact_colors)
        else:
                st.error("Please Enter the classification")
                

if heading == 'Migrate to SQL':
        cursor.execute("select distinct(classification) from artifact_metadata")
        result = cursor.fetchall()
        data_list = [i[0] for i in result]

        st.subheader("Insert the collected data")
        if st.button("Insert"):
          if classification not in data_list:
                  
                  records = object_details(api_key, classification)
                  artifact_metadata,artifact_media,artifact_colors = artifacts_data(records)
                  insert_values(artifact_metadata,artifact_media,artifact_colors)
                  st.success("Data Inserted successfully")

                  st.header("Inserted Data:")
                  st.divider()

                  st.subheader("Artifact_Metadata")
                  cursor.execute("select * from artifact_metadata")
                  table1 = cursor.fetchall()
                  columns = [i[0] for i in cursor.description]
                  df1 = pd.DataFrame(table1,columns=columns)
                  st.dataframe(df1)

                  st.subheader("Artifact_Media")
                  cursor.execute("select * from artifact_media")
                  table2 = cursor.fetchall()
                  columns = [i[0] for i in cursor.description]
                  df2 = pd.DataFrame(table2,columns=columns)
                  st.dataframe(df2)

                  st.subheader("Artifact_Colors")
                  cursor.execute("select * from artifact_colors")
                  table3 = cursor.fetchall()
                  columns = [i[0] for i in cursor.description]
                  df3 = pd.DataFrame(table3,columns=columns)
                  st.dataframe(df3)

          else:
            st.error("Kindly enter other classification ")


elif heading == 'SQL queries':
                            options = st.selectbox("Queries", ["1. List all artifacts from the 11th century belonging to Byzantine culture",
                                            "2. What are the unique cultures represented in the artifacts?",
                                            "3. List all artifacts from the Archaic Period",
                                            "4. List artifact titles ordered by accession year in descending order",
                                            "5. How many artifacts are there per department?",
                                            "6. Which artifacts have more than 3 images?",
                                            "7.What is the average rank_num of all artifacts?",
                                            "8. Which artifacts have a lesser mediacount than colorcount?",
                                            "9. List all artifacts created between 1500 and 1600",
                                            "10. How many artifacts have no media files?",
                                            "11. What are all the distinct hues used in the dataset?",
                                            "12. What are the top 5 most used colors by frequency?",
                                            "13. What is the average coverage percentage for each hue?",
                                            "14. List all colors used for a given artifact ID",
                                            "15. What is the total number of color entries in the dataset?",
                                            "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture",
                                            "17. List each artifact title with its associated hues",
                                            "18. Get artifact titles, cultures, and media ranks where the period is not null",
                                            "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'",
                                            "20. How many artifacts exist per classification, and what is the average media count for each?",
                                            "1. List all artifact from the tables, accessionyear wise (desc)",
                                            "2. Get spectrum value for given id?",
                                            "3. List artifact culture ordered by accessionmethod in ascending order",
                                            "4. how many artifacts are there in medium",
                                            "5. which artifacts have more than 2 colorcount"], placeholder ='Select a Query',index =None)
        
                            if options == "1. List all artifacts from the 11th century belonging to Byzantine culture":
                                            cursor.execute("select * from artifact_metadata where culture = 'Byzantine'")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns = columns)
                                            st.dataframe(data)

                            elif options == "2. What are the unique cultures represented in the artifacts?":
                                            cursor.execute("select distinct (culture) from artifact_metadata")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "3. List all artifacts from the Archaic Period":
                                            cursor.execute("select * from artifact_metadata where period = 'Archaic Period'")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)
                                    
                            elif options == "4. List artifact titles ordered by accession year in descending order":
                                            cursor.execute("select title from artifact_metadata order by accessionyear desc")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "5. How many artifacts are there per department?":
                                            cursor.execute("select department, count(*) as artifact_count from artifact_metadata group by department")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            
                            elif options == "6. Which artifacts have more than 3 images?":
                                            cursor.execute("select * from artifact_media where imagecount > 3")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns = columns)
                                            st.dataframe(data)

                            elif options == "7.What is the average rank_num of all artifacts?":
                                            cursor.execute("select avg(rank_num) from artifact_media")
                                            result = cursor.fetchone()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "8. Which artifacts have a lesser mediacount than colorcount?":
                                            cursor.execute("select * from artifact_media where mediacount < colorcount")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "9. List all artifacts created between 1500 and 1600":
                                            cursor.execute("select * from artifact_media where datebegin between 1500 and 1600")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)  

                            elif options == "10. How many artifacts have no media files?":
                                            cursor.execute("select count(*) as artifact_media_count from artifact_media where mediacount = 0 or mediacount is null")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)                                                           

                            elif options == "11. What are all the distinct hues used in the dataset?":
                                            cursor.execute("select distinct hue from artifact_colors")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns = columns)
                                            st.dataframe(data)

                            elif options == "12. What are the top 5 most used colors by frequency?":
                                            cursor.execute("select color, count(*) from artifact_colors group by color order by count(color) desc limit 5")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "13. What is the average coverage percentage for each hue?":
                                            cursor.execute("select hue, avg(percent) from artifact_colors group by hue")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)
                                            
                            elif options == "14. List all colors used for a given artifact ID":
                                            cursor.execute("select color from artifact_colors where objectid =350732")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "15. What is the total number of color entries in the dataset?":
                                            cursor.execute("select count(color) as color_entries from artifact_colors")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture":
                                            cursor.execute("select am.title, ac.hue from artifact_metadata am, artifact_colors ac where am.id=ac.objectid and am.culture = 'Byzantine' group by am.title, ac.hue")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns = columns)
                                            st.dataframe(data)

                            elif options == "17. List each artifact title with its associated hues":
                                            cursor.execute("select distinct am.title, ac.hue from artifact_metadata am, artifact_colors ac where am.id = ac.objectid order by am.title")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "18. Get artifact titles, cultures, and media ranks where the period is not null":
                                            cursor.execute("select am.title, am.culture, amed.rank_num from artifact_metadata am, artifact_media amed where am.id=amed.objectid and am.period is not null order by amed.rank_num asc")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'":
                                            cursor.execute("select distinct (am.title) from artifact_metadata am, artifact_media amed, artifact_colors ac where am.id = ac.objectid and am.id=amed.objectid and ac.hue='Grey' limit 10")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "20. How many artifacts exist per classification, and what is the average media count for each?":
                                            cursor.execute("select am.classification, count(*), avg(amed.mediacount) from artifact_metadata am, artifact_media amed where am.id = amed.objectid group by am.classification")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "1. List all artifact from the tables, accessionyear wise (desc)":
                                            cursor.execute("select * from artifact_metadata order by accessionyear desc")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns = columns)
                                            st.dataframe(data)

                            elif options == "2. Get spectrum value for given id?":
                                            cursor.execute("select spectrum, count(objectid) as spectrum_value from artifact_colors group by spectrum")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "3. List artifact culture ordered by accessionmethod in ascending order":
                                            cursor.execute("select culture from artifact_metadata order by accessionmethod asc")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "4. how many artifacts are there in medium":
                                            cursor.execute("select medium, count(*) as medium_count from artifact_metadata group by medium")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                            elif options == "5. which artifacts have more than 2 colorcount":
                                            cursor.execute("select * from artifact_media where colorcount")
                                            result = cursor.fetchall()
                                            columns = [i[0] for i in cursor.description]
                                            data = pd.DataFrame(result, columns=columns)
                                            st.dataframe(data)

                                            