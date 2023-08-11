CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE telco_docs(
  content TEXT
);



CREATE TABLE telco_documents (
  id bigserial primary key,
  content text,
  embedding vector(1536)
)
DISTRIBUTED BY (id)
;

CREATE INDEX ON telco_documents USING ivfflat (embedding vector_cosine_ops)
with
  (lists = 300);

-- COPY data to table

COPY telco_documents from '/home/gpadmin/telco_documents.csv' CSV HEADER DELIMITER '|' QUOTE '"' ;

ANALYZE telco_documents;


CREATE OR REPLACE FUNCTION match_documents (
    query_embedding VECTOR(1536),
    match_threshold FLOAT,
    match_count INT
  )

  RETURNS TABLE (
    id BIGINT,
    content TEXT,
    similarity FLOAT
  )

  AS $$

    SELECT
      documents.id,
      documents.content,
      1 - (documents.embedding <=> query_embedding) AS similarity
    FROM telco_documents documents
    WHERE 1 - (documents.embedding <=> query_embedding) > match_threshold
    ORDER BY similarity DESC
    LIMIT match_count;

  $$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_embeddings(content text)
  RETURNS VECTOR
  AS
  $$
  import openai
  import os
  text = content
  openai.api_key = os.getenv("OPENAI_API_KEY")
  response = openai.Embedding.create(
        model="text-embedding-ada-002",
        input = text.replace("\n"," ")
    )

  embedding = response['data'][0]['embedding']
  return embedding

  $$ LANGUAGE PLPYTHON3U;

CREATE FUNCTION ask_openai(user_input text, document text)
  RETURNS TEXT
  AS
  $$

    import openai
    import os

    openai.api_key = os.getenv("OPENAI_API_KEY")
    search_string = user_input
    docs_text = document

    messages = [{"role": "system",
                  "content": "You concisely answer questions based on text that is provided to you."}]

    prompt = """Answer the user's prompt or question:

    {search_string}

    by summarizing the following text:

    {docs_text}

    Keep your answer direct and concise. Provide code snippets where applicable.
    The question is about a Greenplum / PostgreSQL database. You can enrich the answer with other
    Greenplum or PostgreSQL-relevant details if applicable.""".format(search_string=search_string, docs_text=docs_text)

    messages.append({"role": "user", "content": prompt})

    response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages)
    return response.choices[0]["message"]["content"]

  $$ LANGUAGE PLPYTHON3U;


CREATE OR REPLACE FUNCTION intelligent_ai_assistant(
    user_input TEXT
  )

  RETURNS TABLE (
    content TEXT
  )
  LANGUAGE SQL STABLE
  AS $$

    SELECT
      ask_openai(user_input,
                (SELECT t.content
                  FROM match_documents(
                        (SELECT get_embeddings(user_input)) ,
                          0.8,
                          1) t
                  )
      );
$$;


CREATE OR REPLACE FUNCTION intelligent_ai_assistant_bills(
    user_input TEXT
  )
  RETURNS TABLE (
    content TEXT
  )
  LANGUAGE SQL STABLE
  AS $$
    SELECT
      ask_openai(user_input,
                (SELECT t.content
                  FROM match_documents(
                        (SELECT get_embeddings(user_input)) ,
                          0.1,
                          1) t
                  )
      );
$$;