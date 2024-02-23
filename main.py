import streamlit as st
from snowflake.snowpark import Session
from config import snowflake_config
from snowflake.snowpark.functions import col
from config import snowflake_config
import time 




session = Session.builder.configs(snowflake_config).create()
selected_category = st.sidebar.selectbox("Please Pick the category",options =["A", "B", "C"],index=None )
team1_count = session.table('DB_APL.SH_APL.SCREAMING_EAGLES')
team2_count = session.table('DB_APL.SH_APL.GRAY_PANTHERS')
team3_count = session.table('DB_APL.SH_APL.POWER_GLADIATORS')
df_player_info = session.table('DB_APL.SH_APL.PLAYERS_INFO').filter(col("category") == selected_category).to_pandas()


value1=team1_count.agg("AMOUNT_SPENT","sum").to_pandas()
desired_value1 = value1['SUM(AMOUNT_SPENT)'].iloc[0]

value2=team2_count.agg("AMOUNT_SPENT","sum").to_pandas()
desired_value2 = value2['SUM(AMOUNT_SPENT)'].iloc[0]

value3=team3_count.agg("AMOUNT_SPENT","sum").to_pandas()
desired_value3 = value3['SUM(AMOUNT_SPENT)'].iloc[0]



page = st.sidebar.radio("Select Page", ["Buy Players", "captains Information","Team Information"])
player_name_list = df_player_info['PLAYER_NAME'].unique().tolist()

catogary_value_a=700
catogary_value_b=500
catogary_value_c=300

df_team_names = session.table('DB_APL.SH_APL.TEAM_BUDGET').select('TEAM_NAME').to_pandas()
team_names = df_team_names['TEAM_NAME'].unique().tolist()



if page=="Buy Players":
    st.sidebar.table(df_player_info[['PLAYER_NAME', 'PROFILE']])
    col1, col2, col3 = st.columns(3)
    with col1:
                        st.info(f"Screaming Eagles   \n    Player Bought: {team1_count.count()}        \n              Budget left: {15000 - desired_value1}")

    with col2:
                        st.error(f"Gray Panthers   \n    Player Bought: {team2_count.count()}        \n               Budget left: {15000 - desired_value2}")

    with col3:
                        st.warning(f"Power Gladiators  \n    Player Bought: {team3_count.count()}        \n              Budget left: {15000 - desired_value3}")

    if not player_name_list:
        st.error("Please choose category ")
    else:
        with st.form("New Player", clear_on_submit=True):


            

            current_player = df_player_info[df_player_info['PLAYER_NO'] == 1]
            
            player_name = current_player['PLAYER_NAME'].iloc[0]
            st.markdown(
        f"<div style='display: flex; justify-content: center;'><h3 style='font-size: 25px;'>Player Info</h3></div>",
        unsafe_allow_html=True)
                
            st.image("dummy_image.jpg", width=80) 
            
            profile = current_player['PROFILE'].iloc[0]
            category = current_player['CATEGORY'].iloc[0]

            col9, col10 = st.columns(2)
            with col9:
                st.markdown(f"**Player Name:** {player_name}", unsafe_allow_html=True)
            with col10:
                st.markdown(f"**Profile:** {profile}", unsafe_allow_html=True)

        
        
            if category=='A':
                catogary_value=catogary_value_a
                increment=200
            elif category=='B':
                catogary_value=catogary_value_b
                increment=100
            else:
                catogary_value=catogary_value_c 
                increment=100
            st.markdown(
        f"<div style='display: flex; justify-content: center;'><h3 style='font-size: 15px;'>Base Value of Player is {catogary_value}</h3></div>",
        unsafe_allow_html=True)

            col6, col5 = st.columns(2)
            with col6:
                to_amount = st.number_input("Enter the amount", value=catogary_value,step=increment)
            with col5:
                to_team = st.selectbox("Select the team", team_names,index=None, placeholder="Choose an option")

            if st.form_submit_button("Close Bid ") and to_team is not None and to_amount is not None:
                query_insert = f"insert into DB_APL.SH_APL.{to_team} (PLAYER_NAME,PROFILE,EMAIL_ID,PHONE_NUMBER,AMOUNT_SPENT ) select player_name,PROFILE,EMAIL_ID,PHONE_NUMBER,{to_amount} from  DB_APL.SH_APL.PLAYERS_INFO where player_name='{player_name}'"   
                query_delete = f"delete from DB_APL.SH_APL.PLAYERS_INFO where player_name='{player_name}'"
                query_update=(f"update DB_APL.SH_APL.PLAYERS_INFO set PLAYER_NO=PLAYER_NO-1 where PLAYER_NO>1 and category='{selected_category}'")
                # Execute the queries
                session.sql(query_insert).collect()
                session.sql(query_delete).collect()
                session.sql(query_update).collect()

                st.success(f"Congratulations! {to_team} has successfully acquired {player_name}. Well done")
                st.balloons()
                
                #time.sleep(5)
                if st.form_submit_button('Next Player'):
                    st.rerun()
            # else:
            #       st.error('Please enter selected team')
if page=='captains Information':
        st.title("Interested Captains")
        st.write("The Base price of Captains is 700")
        cap_info = session.table('DB_APL.SH_APL.PLAYERS_INFO').filter(col("category") == "Captain").to_pandas()
        
        st.dataframe(cap_info[['PLAYER_NAME','PROFILE',]])
if page=='Team Information':
        team1 = session.table('DB_APL.SH_APL.SCREAMING_EAGLES')
        team2 = session.table('DB_APL.SH_APL.GRAY_PANTHERS')
        team3 = session.table('DB_APL.SH_APL.POWER_GLADIATORS')

        if st.button("SCREAMING_EAGLES"):
              st.dataframe(team1)
        if st.button("GRAY_PANTHERS"):
              st.dataframe(team2)
        if st.button("POWER_GLADIATORS"):
              st.dataframe(team3)
              
            

            

