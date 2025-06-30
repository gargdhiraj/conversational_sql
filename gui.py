import streamlit as st

st.write("# Welcome to the Streamlit App")

user_query = st.text_input("Enter your query", key="query_input")

st.write("Your query is :", user_query)
