drop schema search cascade;
create schema search;
grant usage on schema search to server;
grant execute on all functions in schema search to server;


create type search.top_donors as (total integer, contributor_payee text, filer text);

create or replace function search.top_donors(auser text, apath jsonb, args jsonb)
  returns setof search.top_donors as $$

  select (sum(amount), contributor_payee, filer)::search.top_donors
  from working_transactions 
  where sub_type = 'Cash Contribution' and filer_id in 
          (select distinct filer_id from working_transactions, working_committees where
           filer_id = committee_id
           and committee_type = 'CC'
           and contributor_payee ilike '%' || (apath->>0) || '%')
  group by contributor_payee, filer order by contributor_payee, sum(amount) desc;

$$ language sql;
