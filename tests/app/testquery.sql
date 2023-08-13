--- test cases 

select ask_openai('what is Greenplum?');

select ask_openai('give me all mobile plans available in Telstra');

select * from intelligent_ai_assistant('give me all mobile plans available in Telstra');
select * from intelligent_ai_assistant('what is the best mobile plan for me? I need around 100GB data and unlimited international calls to China per month
However, I want to pay as less as possible.') ;

select * from intelligent_ai_assistant('give me all invoices over $100') ;

-- 17 invoices
select t.filename, t.content from match_docs(
  (select get_embeddings('give me all invoices over $100')), 0.765, 100) t;

-- 0 result
select t.filename, t.content from match_docs((select 
get_embeddings('give me all customer names who have invoices over $100')), 0.765, 100) t;

-- 17 invoices and customer names
select distinct t.filename, cust_full_name, email_address, full_address from match_docs_customer_info 
((select get_embeddings('give me all customer names who have invoices over $100')), 0.75, 100)t
order by 2, 1;



---------------------------------------------
-- Deep Dive
---------------------------------------------

  SELECT
      documents.id,
      documents.filename,
      1 - (documents.embedding <=> (select get_embeddings('give me all invoices over $100'))) AS similarity,
      c.c_customer_sk, 
      c.c_first_name, 
      c.c_last_name,
      c.c_email_address,
      addr.ca_street_number, 
      addr.ca_street_name, 
      addr.ca_street_type, 
      addr.ca_suite_number, 
      addr.ca_city, 
      addr.ca_county, 
      addr.ca_state, 
      addr.ca_zip, 
      addr.ca_country,
      documents.content
    FROM telco_doc documents
    -- join to customer table
    inner join customer_invoice ci 
	on documents.filename = ci.filename 
	inner join customer c 
	on ci.customer_sk = c.c_customer_sk
	left join customer_address addr 
	on c.c_current_addr_sk = addr.ca_address_sk
	 -- WHERE 1 - (documents.embedding <=> query_embedding) > match_threshold
    ORDER BY similarity DESC
   -- LIMIT match_count;,


-- customer & address info
select c_current_addr_sk, c_customer_sk, 
c_first_name, c_last_name, c_email_address,
ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country
from customer c
inner join customer_address addr 
on c.c_current_addr_sk = addr.ca_address_sk
where c_customer_sk between 15 and 20
order by c_customer_sk;
