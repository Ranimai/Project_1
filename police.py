import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector as mysqlcon

# Connection SQL
def create_connection():
    try:
        connection = mysqlcon.connect(host='localhost',user='root', password='Rani', database = 'policelogs')
        cursor = connection.cursor(dictionary=True)
        return connection
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return None
    
 # Fetch data from database
def fetch_data(query):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            return df
        finally:
            connection.close()
    else:
        return pd.DataFrame()
    
st.title("👮‍♂️ SecureCheck: A Python-SQL Digital Ledger for Police Post Logs")
st.divider()
st.text("Real-time monitoring and insights for law enforcement 🚗")

st.balloons()

st.header("📋 Police Logs Overview")

df = pd.read_csv("traffic_stops - traffic_stops_with_vehicle_number.csv")
st.write(df)

st.header("📊 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stops = df.shape[0]
    st.metric("Total Police Stops", total_stops)

with col2:
    arrests = df[df["stop_outcome"].str.contains("arrest", case=False, na= False)].shape[0]
    st.metric("Total Arrests", arrests)

with col3:
    warnings = df[df["stop_outcome"].str.contains("warnings", case=False, na= False)].shape[0]
    st.metric("Total Warnings",warnings)

with col4:
    drug_related = df[df["drugs_related_stop"] == 1].shape[0] 
    st.metric("Drug_Related_stops", drug_related)   

st.header("🔍 Advanced Insights")

selected_query = st.selectbox("Queries", ["What are the top 10 vehicle_Number involved in drug-related stops?",
                                   "Which vehicles were most frequently searched?",
                                   "Which driver age group had the highest arrest rate?",
                                   "What is the gender distribution of drivers stopped in each country?",
                                   "Which race and gender combination has the highest search rate?",
                                   "What time of day sees the most traffic stops?",
                                   "What is the average stop duration for different violations?",
                                   "Are stops during the night more likely to lead to arrests?",
                                   "Which violations are most associated with searches or arrests?",
                                   "Which violations are most common among younger drivers (<25)?",
                                   "Is there a violation that rarely results in search or arrest?",
                                   "Which countries report the highest rate of drug-related stops?",
                                   "What is the arrest rate by country and violation?",
                                   "Which country has the most stops with search conducted?",
                                   "Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)",
                                   "Driver Violation Trends Based on Age and Race (Join with Subquery)",
                                   "Time Period Analysis of Stops (Joining with Date Functions), Number of Stops by Year, Month, Hour of the Day",
                                   "Violations with High Search and Arrest Rates (Window Function)",
                                   "Driver Demographics by Country (Age, Gender, and Race)",
                                   "Top 5 Violations with Highest Arrest Rates"], placeholder ='Select a Query',index =None)


insert_query = {"What are the top 10 vehicle_Number involved in drug-related stops?": "select vehicle_number, count(*) as stop_count from public_safety where drugs_related_stop = True group by vehicle_number order by stop_count asc limit 10;",
                "Which vehicles were most frequently searched?": "select vehicle_number, count(*) as searched from public_safety where search_conducted = 1 group by vehicle_number order by searched desc limit 10;",
                "Which driver age group had the highest arrest rate?": "select driver_age, count(*) as arrest_rate from public_safety group by driver_age order by arrest_rate desc limit 5;",
                "What is the gender distribution of drivers stopped in each country?": "select country_name, driver_gender, count(*) as count from public_safety group by country_name, driver_gender;",
                "Which race and gender combination has the highest search rate?": "select driver_race, driver_gender, count(*) as search_rate from public_safety where search_conducted = True group by driver_race, driver_gender;",
                "What time of day sees the most traffic stops?": "select stop_date, stop_time, count(*) as total_stops from public_safety group by  stop_date, stop_time;",
                "What is the average stop duration for different violations?": "select violation, avg(stop_duration) from public_safety group by violation;",
                "Are stops during the night more likely to lead to arrests?": "select case when stop_time between '6:00:00' and '18:00:00' then 'day' else 'Night' end as arrest from public_safety group by arrest;",
                "Which violations are most associated with searches or arrests?": "select violation, count(*) as total_arrest from public_safety where stop_outcome like '%arrest%' group by violation;",
                "Which violations are most common among younger drivers (<25)?": "select violation, count(*) as total_cases from public_safety where driver_age < 25 group by violation;",
                "Is there a violation that rarely results in search or arrest?": "select violation, count(*) as count, SUM(CASE WHEN search_conducted = True then 1 else 0 end) as total_search, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests from public_safety group by violation;",
                "Which countries report the highest rate of drug-related stops?": "select country_name, count(*) as highest_rate from public_safety where drugs_related_stop = True group by country_name order by highest_rate;",
                "What is the arrest rate by country and violation?": "select country_name, violation, round(sum(case when is_arrested = True then 1 else 0 end)*100/count(*), 2) as count from public_safety group by country_name, violation order by count desc;",
                "Which country has the most stops with search conducted?": "select country_name, count(*) as count from public_safety where search_conducted= True group by country_name order by count desc;",
                "Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)": "select country_name, yearly_breakdown, SUM(total_stops) OVER (PARTITION BY country_name) AS total_stops, SUM(total_arrests) OVER (PARTITION BY country_name) AS total_arrests from (select country_name, count(*) as total_stops, extract(year from stop_date) as yearly_breakdown, sum(case when is_arrested = True then 1 else 0 end) as total_arrests from public_safety group by country_name, extract(year from stop_date)) as yearly_data order by country_name, yearly_breakdown;",
                "Driver Violation Trends Based on Age and Race (Join with Subquery)": "select distinct v.violation, ps.driver_age, ps.driver_race from public_safety ps join (select driver_age, driver_race, count(*) as violation from public_safety group by driver_age, driver_race) as v on ps.driver_age = v.driver_age and ps.driver_race = v.driver_race order by v.violation desc;",
                "Time Period Analysis of Stops (Joining with Date Functions), Number of Stops by Year, Month, Hour of the Day": "select year(stop_date) as stop_year, month(stop_date) as stop_month, hour(stop_time) as stop_hour, count(*) as Number_of_stops from public_safety group by stop_year, stop_month, stop_hour;",
                "Violations with High Search and Arrest Rates (Window Function)": "select violation, count(*) as total_stops, sum(case when search_conducted = True then 1 else 0 end) as total_search, sum(case when is_arrested = True then 1 else 0 end) as total_arrest, rank() over (order by sum(case when search_conducted = True then 1 else 0 end)* 1.0 / Count(*)) as search, rank() over (order by sum(case when is_arrested = True then 1 else 0 end)* 1.0/ count(*)) as arrest from public_safety group by violation;",
                "Driver Demographics by Country (Age, Gender, and Race)": "select country_name, driver_age, driver_gender, driver_race, count(*) as total_drivers from public_safety group by country_name, driver_age, driver_gender, driver_race order by country_name, driver_age, driver_gender, driver_race;",
                "Top 5 Violations with Highest Arrest Rates": "select violation, count(*) as total_stops, sum(case when is_arrested = True then 1 else 0 end) as total_arrest, round(sum(case when is_arrested = True then 1 else 0 end) *1.0/ count(*), 2) as arrest_rate from public_safety group by violation order by total_arrest desc limit 5;"
}

if st.button("Run Query"):
    result = fetch_data(insert_query[selected_query])
    if not result.empty:
        st.dataframe(result)
    else:
        st.warning("No results are found for the options.")

st.divider()


st.header("📋 Display the Predict outcome and Violation")

# Input form for all fields(excluding outputs)
with st.form("new_log_form"):
    stop_date = st.date_input("stop Date")
    stop_time = st.time_input("stop Time")
    country_name = st.text_input("Country Name")
    driver_gender = st.selectbox("Driver Gender", ['Male','Female'])
    driver_age = st.number_input("Driver Age", min_value= 16, max_value= 100, value= 27)
    driver_race = st.text_input("Driver Race")
    search_conducted = st.selectbox("Was a Search Conducted?", ["0","1"])
    search_type = st.text_input("Search Type")
    drugs_related_stop = st.selectbox("Was it Drugs Related", ["0","1"])
    stop_duration = st.selectbox("Stop Duration", df['stop_duration'].dropna().unique())
    vehicle_number = st.text_input("Vehicle Number")
    timestamp = pd.Timestamp.now()

    submitted = st.form_submit_button("🕵️ Predict Stop Outcome and Violation")

# Filter data for prediction
    if submitted:
        filter_data = df[
            (df['driver_gender'] == driver_gender) &
            (df['driver_age'] == driver_age) &
            (df['search_conducted'] == search_conducted) &
            (df['stop_duration'] == stop_duration) &
            (df['drugs_related_stop'] == int(drugs_related_stop))
        ]

        # Predict stop outcome
        if not filter_data.empty:
            predicted_outcome = filter_data['stop_outcome'].mode()[0]
            predicted_violation = filter_data['violation'].mode()[0]
        else:
            predicted_outcome = "Warning" 
            predicted_violation = "Speeding"

        # Natural Language Summary
        Search_text = "A Search was Conducted" if int(search_conducted) else "No search was conducted"
        drug_text = "was drug_related" if int(drugs_related_stop) else "was not 💊 drug_related"

        st.markdown(f"""
                    **Prediction Summary**
                    - **Predicted Violation:** {predicted_violation}
                    - **Predicted Stop Outcome:** {predicted_outcome}
                    
                    🚓 A  **{driver_age}**-year-old 🧍‍♂️ **{driver_gender}** driver in 🌎 **{ country_name}** was stopped at 🕒**{stop_time.strftime('%I:%M%p')}** on 📅**{stop_date}**. 
                    {Search_text}, and the stop {drug_text}.
                    stop duration: **{stop_duration}**.
                    Vehicle Number: **{vehicle_number}**.
                    """)

