import streamlit as st
import time
from snowflake.snowpark import Session
from config import snowflake_config
from snowflake.snowpark.functions import col
from config import snowflake_config
import random 
from streamlit_modal import Modal


session = Session.builder.configs(snowflake_config).create()
selected_category = st.sidebar.selectbox("Please Pick the category", ["A", "B", "C","CAP"])
team1_count=session.table('DB_APL.SH_APL.STREAMING_EAGLES')
team2_count=session.table('DB_APL.SH_APL.GRAY_PANTHERS')
team3_count=session.table('DB_APL.SH_APL.POWER_GLADIATORS')
						
					  
						



value1=team1_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value1 = value1['SUM(AMOUNT_LEFT)'].iloc[0]

value2=team2_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value2 = value2['SUM(AMOUNT_LEFT)'].iloc[0]

value3=team3_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value3 = value3['SUM(AMOUNT_LEFT)'].iloc[0]





df_player_info =session.table('DB_APL.SH_APL.PLAYERS_INFO').filter(col("category")==selected_category).to_pandas()

player_name_list =df_player_info['PLAYER_NAME'].unique().tolist()
st.sidebar.table(df_player_info[['PLAYER_NAME', 'PROFILE']])

df_team_names =session.table('DB_APL.SH_APL.TEAM_BUDGET').select('TEAM_NAME').to_pandas()
team_names=df_team_names['TEAM_NAME'].unique().tolist()

catogary_value_a=700
catogary_value_b=500
catogary_value_c=300

if not player_name_list:
    st.error("Category players are completed")
else:
    with st.form("New Player",clear_on_submit=True):
        
        col1, col2, col3 = st.columns(3)
        with col1:
                    st.info(f"Screaming Eagles   \n    Player Bought: {team1_count.count()}        \n              Budget left: {10000 - desired_value1}")

        with col2:
                    st.error(f"Gray Panthers   \n    Player Bought: {team2_count.count()}        \n               Budget left: {10000 - desired_value2}")

        with col3:
                    st.warning(f"Power Gladiators  \n    Player Bought: {team3_count.count()}        \n              Budget left: {10000 - desired_value3}")





        
        
        random_name=random.choice(player_name_list)
        current_player=df_player_info[df_player_info['PLAYER_NAME']==random_name]

        st.title("Player Info")
              
        st.image("dummy_image.jpg", width=80)  # Adjust the width value to your preference


        st.session_state.player_name = current_player['PLAYER_NAME'].iloc[0]
        profile = current_player['PROFILE'].iloc[0]
        st.write(st.session_state.player_name)
																 
															 
        category = current_player['CATEGORY'].iloc[0]

        st.markdown(f"**Player Name:** {st.session_state.player_name}", unsafe_allow_html=True)
																	  
																	  
																					
        st.markdown(f"**Profile:** {profile}", unsafe_allow_html=True)
        
        st.write(st.session_state.player_name)
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
    f"<div style='display: flex; justify-content: center;'><h3 style='font-size: 18px;'>Base Value of Player is {catogary_value}</h3></div>",
    unsafe_allow_html=True
)



        to_team=None
        col6,col5= st.columns(2)
        with col6:
            to_amount = st.number_input("Present bit amount",value=catogary_value,step=increment)
        with col5:
            to_team = st.selectbox("Select the team", team_names, key=None, placeholder="Choose an option")
            
            
        
  
        if st.form_submit_button("Next Player",) and to_team !=None and to_amount != None  :
            
            st.write(st.session_state.player_name)
            query_insert=f"insert into DB_APL.SH_APL.{to_team} (player_name,PROFILE,EMAIL_ID,PHONE_NUMBER,AMOUNT_LEFT ) select player_name,PROFILE,EMAIL_ID,PHONE_NUMBER,{to_amount} from  DB_APL.SH_APL.PLAYERS_INFO where player_name='{random_name}'"        
            query_delete=f"delete from DB_APL.SH_APL.PLAYERS_INFO where player_name='{random_name}'"
            session.sql(query_insert).collect()
            session.sql(query_delete).collect()


            st.balloons()
            st.success(f"{to_team} bought {st.session_state.player_name} ,Congratulations")
					  

            
      
            



