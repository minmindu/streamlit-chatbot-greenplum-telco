--- test cases 

select ask_openai('what is Greenplum?');

select ask_openai('give me all mobile plans available in Telstra');

select * from intelligent_ai_assistant('give me all mobile plans available in Telstra');
select * from intelligent_ai_assistant('what is the best mobile plan for me? I need around 100GB data and unlimited international calls to China per month
However, I want to pay as less as possible.') ;

select * from intelligent_ai_assistant('give me all invoices over $100') ;

-- 16 invoices
select t.filename, t.content from match_docs((select get_embeddings('give me all invoices over $100')), 0.765, 100) t;

-- 0 result
select t.filename, t.content from match_docs((select get_embeddings('give me all customer names who have invoices over $100')), 0.765, 100) t;
