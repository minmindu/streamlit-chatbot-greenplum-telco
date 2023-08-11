CREATE DATABASE telco IF NOT EXISTS;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS PLPYTHON3U;


CREATE TABLE customer (
	c_customer_sk int4 NOT NULL,
	c_customer_id bpchar(16) NOT NULL,
	c_current_cdemo_sk int4 NULL,
	c_current_hdemo_sk int4 NULL,
	c_current_addr_sk int4 NULL,
	c_first_shipto_date_sk int4 NULL,
	c_first_sales_date_sk int4 NULL,
	c_salutation bpchar(10) NULL,
	c_first_name bpchar(20) NULL,
	c_last_name bpchar(30) NULL,
	c_preferred_cust_flag bpchar(1) NULL,
	c_birth_day int4 NULL,
	c_birth_month int4 NULL,
	c_birth_year int4 NULL,
	c_birth_country varchar(20) NULL,
	c_login bpchar(13) NULL,
	c_email_address bpchar(50) NULL,
	c_last_review_date bpchar(10) NULL,
	CONSTRAINT customer_pkey PRIMARY KEY (c_customer_sk)
)
DISTRIBUTED BY (c_customer_sk);


CREATE TABLE customer_address (
    ca_address_sk integer NOT NULL,
    ca_address_id character varying(16) NOT NULL,
    ca_street_number character varying(10),
    ca_street_name character varying(60),
    ca_street_type character varying(15),
    ca_suite_number character varying(10),
    ca_city character varying(60),
    ca_county character varying(30),
    ca_state character varying(2),
    ca_zip character varying(10),
    ca_country character varying(20),
    ca_gmt_offset numeric(5,2),
    ca_location_type character varying(20)
)
DISTRIBUTED BY (ca_address_sk);


CREATE TABLE invoice (
  id bigserial primary key,
  filename text,
  content text,
  embedding vector(1536)
)
DISTRIBUTED BY (id)
;

CREATE TABLE customer_invoice(
  id bigserial,
  customer_sk int4 NOT NULL,
  invoice_id BIGINT NOT NULL, 
  filename TEXT
)
DISTRIBUTED BY (customer_sk);

-- COPY data to table

COPY customer  from '/home/gpadmin/test_data/telcodocs/customer.csv' CSV  DELIMITER '|'  ;
COPY customer_address from '/home/gpadmin/test_data/telcodocs/customer_address.csv' CSV  DELIMITER '|' ;

COPY invoice (filename, content, embedding) from '/home/gpadmin/test_data/telcodocs/invoice_embedding.csv' CSV HEADER DELIMITER '|' QUOTE '"' ;


-- manually insert data to customer_invoice 
-- for customer_sk = 1 
insert into customer_invoice(customer_sk, invoice_id, filename)
select 1, i.id, i.filename
from invoice i
where filename like 'invoice1100%';

insert into customer_invoice(customer_sk, invoice_id, filename)
select 2, i.id, i.filename
from invoice i
where filename like 'invoice2100%';

insert into customer_invoice(customer_sk, invoice_id, filename)
select 3, i.id, i.filename
from invoice i
where filename like 'invoice1101%';

insert into customer_invoice(customer_sk, invoice_id, filename)
select 4, i.id, i.filename
from invoice i
where filename like 'invoice1102%';

insert into customer_invoice(customer_sk, invoice_id, filename)
select 5, i.id, i.filename
from invoice i
where filename like 'invoice2101%';

insert into customer_invoice(customer_sk, invoice_id, filename)
select 6, i.id, i.filename
from invoice i
where filename like 'invoice2102%';



--###########################
-- TODO

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