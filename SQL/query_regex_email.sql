select distinct SUBSTRING_INDEX(email, '@', -1) emails  from tabela.`usuario` u order by emails asc;

# EMAILS Buscaso

select distinct SUBSTRING_INDEX(email, '@', -1) emails  from tabela.`usuario` 
where UPPERSUBSTRING_INDEX(email, '@', -1)  rlike 'GMAIL'
 order by emails asc;
