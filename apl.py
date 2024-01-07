import streamlit as st
from snowflake.snowpark import Session
from config import snowflake_config
from snowflake.snowpark.functions import col
from config import snowflake_config
import random 

session = Session.builder.configs(snowflake_config).create()
selected_category = st.sidebar.selectbox("Please Pick the category", ["A", "B", "C"])
team1_count=session.table('DB_APL.SH_APL.TEAM1')
team2_count=session.table('DB_APL.SH_APL.TEAM2')
team3_count=session.table('DB_APL.SH_APL.TEAM3')
team4_count=session.table('DB_APL.SH_APL.TEAM4')

value1=team1_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value1 = value1['SUM(AMOUNT_LEFT)'].iloc[0]

value2=team2_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value2 = value2['SUM(AMOUNT_LEFT)'].iloc[0]

value3=team3_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value3 = value3['SUM(AMOUNT_LEFT)'].iloc[0]

value4=team4_count.agg("AMOUNT_LEFT","sum").to_pandas()
desired_value4 = value4['SUM(AMOUNT_LEFT)'].iloc[0]








df_player_info =session.table('DB_APL.SH_APL.PLAYERS_INFO').filter(col("category")==selected_category).to_pandas()
player_name_list =df_player_info['PLAYER_NAME'].unique().tolist()
df_team_names =session.table('DB_APL.SH_APL.TEAM_BUDGET').select('TEAM_NAME').to_pandas()
team_names=df_team_names['TEAM_NAME'].unique().tolist()
# player_name_list = [row['PLAYER_NAME'] for row in df_players_name]
catogary_value_a=300
catogary_value_b=200
catogary_value_c=100
#st.write(team_names)
if not player_name_list:
    st.error("Category players are completed")
else:
    with st.form("New Player",clear_on_submit=True):
        
        # col1, col2, col3, col4 = st.columns(4)
        # with col1:
        #     st.success(f" TEAM1   \n    Player Bought: {team1_count.count()}\n  Budget left: {10000 - desired_value1}")
        # with col2:
        #     st.success(f" TEAM2   \n    Player Bought: {team2_count.count()}\n   Budget left: {10000 - desired_value2}")

        # with col3:
        #     st.success(f" TEAM3   \n    Player Bought: {team3_count.count()}\n  Budget left: {10000 - desired_value3}")

        # with col4:
        #     st.success(f" TEAM4   \n    Player Bought: {team4_count.count()}\n   Budget left: {10000 - desired_value4}")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.info(f" TEAM1   \n    Player Bought: {team1_count.count()}\n  Budget left: {10000 - desired_value1}")

        with col2:
            st.error(f" TEAM2   \n    Player Bought: {team2_count.count()}\n   Budget left: {10000 - desired_value2}")

        with col3:
            st.warning(f" TEAM3   \n    Player Bought: {team3_count.count()}\n  Budget left: {10000 - desired_value3}")

        with col4:
            # st.markdown(
            #     f"<div style='color: #ffcc00;'> TEAM4   <br> Player Bought: {team4_count.count()}<br> Budget left: {10000 - desired_value4}</div>",
            #     unsafe_allow_html=True
            # )
            st.success(f" TEAM4   \n    Player Bought: {team4_count.count()}\n   Budget left: {10000 - desired_value4}")

        random_name=random.choice(player_name_list)
        current_player=df_player_info[df_player_info['PLAYER_NAME']==random_name]

        st.title("Player Info")
                # Assuming 'image_path' contains the path to your image file
        image_path = r"C:\Users\saicharan.madikonda\Desktop\Streamlit_pro\APL_auctions\dummy_image.png"

        # Display the image
        st.image(image_path, use_column_width=True)

        player_name = current_player['PLAYER_NAME'].iloc[0]
        batting = current_player['BATTING'].iloc[0]
        bowling = current_player['BOWLING'].iloc[0]
        wicket_keeping = current_player['WICKET_KEEPING'].iloc[0]
        cricheros_id = current_player['CRICHEROS_ID'].iloc[0]
        category = current_player['CATEGORY'].iloc[0]

        st.markdown(f"**Player Name:** {player_name}", unsafe_allow_html=True)
        st.markdown(f"**Batting:** {batting}", unsafe_allow_html=True)
        st.markdown(f"**Bowling:** {bowling}", unsafe_allow_html=True)
        st.markdown(f"**Wicket Keeper:** {wicket_keeping}", unsafe_allow_html=True)
        st.markdown(f"**Cricherosid:** {cricheros_id}", unsafe_allow_html=True)
        #st.markdown(f"**Category:** {category}", unsafe_allow_html=True)
        
        if category=='A':
            catogary_value=catogary_value_a
        elif category=='B':
            catogary_value=catogary_value_b
        else:
            catogary_value=catogary_value_c 
        st.markdown(
    f"<div style='display: flex; justify-content: center;'><h3 style='font-size: 24px;'>Base Value of Player is {catogary_value}</h3></div>",
    unsafe_allow_html=True
)


        #st.markdown(f"**Base Value of Player is {catogary_value}**")
        to_team=None
        col5, col6= st.columns(2)
        with col5:
            to_team = st.selectbox("Select the team", team_names, index=0, key=None, placeholder="Choose an option")
            st.session_state.to_team=to_team
            # Display the selected team
            #st.write(f"Selected Team: {to_team}")
            
        with col6:
            to_amount = st.number_input("Enter the amount",value=catogary_value,step=100)
  
        if st.form_submit_button("Next Player",) and st.session_state.to_team !=None and to_amount != None:
            # st.write(st.session_state.to_team)
            # st.write(to_amount)

            query_insert=f"insert into DB_APL.SH_APL.{st.session_state.to_team} (player_name,batting,bowling,WICKET_KEEPING,CRICHEROS_ID,amount_left ) select player_name,batting,bowling,WICKET_KEEPING,CRICHEROS_ID,{to_amount} from  DB_APL.SH_APL.PLAYERS_INFO where player_name='{random_name}'"        
            query_delete=f"delete from DB_APL.SH_APL.PLAYERS_INFO where player_name='{random_name}'"
            session.sql(query_insert).collect()
            session.sql(query_delete).collect()
            st.rerun()
            to_team=None
            to_amount=None
            st.success("Congradulations")
            
