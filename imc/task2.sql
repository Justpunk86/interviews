--task2 created by Sergey Ermachkov
drop schema if exists test cascade;

create schema test;

set search_path =  public, test, "$user";

\set AUTOCOMMIT FALSE

create sequence if not exists test.ids_seq 
as bigint
increment by 1
minvalue 1
start with 1 
no cycle;

-- создаётся таблица для загрузки исходных данных
-- далее данные будут очищены от дублей
create table test.download_dic(textcode varchar(16), name varchar(128));

create table test.amb(
CKEY		numeric(6),	
CREF		numeric(6),	
CNUM		numeric(6),
CEND		numeric(1),
SKIND		numeric(6),
TEXTCODE	varchar(16),	
NAME		varchar(128),
constraint amb_ckey_pk primary key (ckey)
);

-- создаётся индекс для поиска по столбцу textcode
create index if not exists amb_textcode on test.amb(textcode);

-- выполняется скрипт для загрузки исходных данных
\ir 'download_amb_dic.sql'

-- загружаются данные в таблицу test.amb исключая дубли
insert into test.amb (CKEY, CREF, CNUM, CEND, SKIND, TEXTCODE, NAME)
select
	nextval('test.ids_seq'),
	null,
	null,
	null,
	2,
	t.textcode,
	t.name
from 
(select distinct textcode, name
from test.download_dic) t; 

commit;

-- функция для вычисления родительского значения для textcode
-- из входного textcode выделаяется подстрока с помощью реглярного выражения:
-- с конца строки выделяется часть текста соотв-я '.' и цифрам
create or replace function test.get_parent_code(in_tcode text) returns text
as $$
	select 
		substring(in_tcode from 1 for 
					length(in_tcode) - length(substring(in_tcode,'\d+\.$')));
$$ language sql;

-- ф-я для вычисления з-я для cnum
-- повторно выделяется подстрока: сначала с конца строки, потом убирается '.'
create or replace function test.get_cnum(in_tcode text) returns integer
as $$
	select substring(substring(in_tcode,'\d+\.$'),'^\d+')::integer;		
$$ language sql;

-- ф-я для вычисления cref
create or replace function test.get_cref(in_tcode text) returns integer
as $$
	select distinct
		coalesce(i.ckey, 0)
		as cref
	from test.amb t
	left join test.amb i
		on i.textcode = test.get_parent_code(t.textcode)
	where t.textcode = in_tcode;
$$ language sql;

-- ф-я для вычисления cend
-- входные пар-ы textcode, arr_list - массив со значениями textcode
-- для сокращения обращений к таблице
create or replace function test.get_cend(in_tcode text, tcode_list text[]) returns integer
as $$
	select 
		case 
		when count(code) > 0 then 1
		else 0
		end
	from unnest(tcode_list) AS t(code)
	where code != in_tcode
	and test.get_parent_code(code) = in_tcode;
$$ language sql;

create or replace procedure test.upd_amb()
as $$
declare
tcode_list text[]; 
cur cursor for select textcode, ckey from test.amb;
begin
	tcode_list := (select array(select t.textcode from test.amb t));
	
	-- в цикле по курсору перебираются строки
	-- и обновляются данные
	for i in cur
	loop
		update test.amb 
		set cref =  test.get_cref(i.textcode),
			cnum = test.get_cnum(i.textcode),
			cend = test.get_cend(i.textcode, tcode_list)
		where ckey = i.ckey;
	end loop;
--	commit;

end;
$$ language plpgsql;

call test.upd_amb();
commit;

select * from test.amb order by textcode;




