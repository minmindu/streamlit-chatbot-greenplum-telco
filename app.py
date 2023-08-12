import openai
import streamlit as st
from streamlit_chat import message
import psycopg2
import pandas as pd
import os

conn = psycopg2.connect("postgres://gpadmin:changeme@localhost:5432/warehouse")

# Setting page title and header
st.set_page_config(page_title="Tanzu AI-Assistant: Greenplum-pgVector", page_icon=":robot_face:")
st.markdown("<h2 style='text-align: center;'>Ask me anything about VMware Data Solutions & Tanzu Products 🤖</h2>", unsafe_allow_html=True)

# Set org ID and API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialise session state variables
if 'generated' not in st.session_state:
    st.session_state['generated'] = []
if 'past' not in st.session_state:
    st.session_state['past'] = []
if 'messages' not in st.session_state:
    st.session_state['messages'] = [
        {"role": "system", "content": "You are a helpful assistant."}
    ]
if 'model_name' not in st.session_state:
    st.session_state['model_name'] = []
if 'cost' not in st.session_state:
    st.session_state['cost'] = []
if 'total_tokens' not in st.session_state:
    st.session_state['total_tokens'] = []
if 'total_cost' not in st.session_state:
    st.session_state['total_cost'] = 0.0

st.sidebar.title("Sidebar")
model_name = st.sidebar.radio("Choose a method:", ("Similarity Search using pgvector extension", "OpenAI model (without GP context)", "OpenAI model (summarized + enriched context)"))
counter_placeholder = st.sidebar.empty()
clear_button = st.sidebar.button("Clear Conversation", key="clear")

print('Model name:', model_name)
if model_name == "Similarity Search using pgvector extension":
    function = "match_docs"
elif model_name == "OpenAI model (without GP context)":
    function="ask_openai"
else:
    function = "intelligent_ai_assistant"

# reset everything
if clear_button:
    st.session_state['generated'] = []
    st.session_state['past'] = []
    st.session_state['messages'] = [
        {"role": "system", "content": "You are a helpful assistant."}
    ]
    st.session_state['number_tokens'] = []
    st.session_state['model_name'] = []
    st.session_state['cost'] = []
    st.session_state['total_cost'] = 0.0
    st.session_state['total_tokens'] = []


# generate a response
def generate_response(prompt):
    print(function)
    st.session_state['messages'].append({"role": "user", "content": prompt})

    if function == 'intelligent_ai_assistant':
        response = pd.read_sql("select intelligent_ai_assistant('{input_text}');".format(input_text=prompt), conn).values[0][0]
    elif function=="ask_openai":
        response = pd.read_sql("select ask_openai('{input_text}');".format(input_text=prompt), conn).values[0][0]
    else:
        response = pd.read_sql("select t.filename, t.content from match_docs((select get_embeddings('{input_text}')), 0.765, 100) t;".format(input_text=prompt), conn).values[0][0]

    st.session_state['messages'].append({"role": "assistant", "content": response})

    return response


# container for chat history
response_container = st.container()
# container for text box
container = st.container()

with container:
    with st.form(key='my_form', clear_on_submit=True):
        user_input = st.text_area("You:", key='input', height=100)
        submit_button = st.form_submit_button(label='Send')

    if submit_button and user_input:
        output = generate_response(user_input)
        st.session_state['past'].append(user_input)
        st.session_state['generated'].append(output)
        st.session_state['model_name'].append(model_name)


if st.session_state['generated']:
    with response_container:
        for i in range(len(st.session_state['generated'])):
            message(st.session_state["past"][i], is_user=True, key=str(i) + '_user')
            message('Answer:', key=str(i))
            st.markdown(st.session_state["generated"][i])
            st.write(
                f"Method used: {st.session_state['model_name'][i]};")
