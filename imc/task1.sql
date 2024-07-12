--task1 created by Sergey Ermachkov 
-- удаляется схема со всеми объектами в ней
drop schema if exists test cascade;

create schema test;

set search_path =  public, test, "$user";

\set AUTOCOMMIT FALSE

-- последовательность для первичного ключа
create sequence if not exists test.ids_seq 
as bigint
increment by 1
minvalue 1
start with 1 
no cycle;

-- создание таблиц
-- для реализации связи в таблицы добавлены
-- столбцы для ссылки и внешние ключи:
-- id_schet, id_sluch для таблиц sluch, usl соответственно
-- в таблицы добавлен первичный ключ для столбца id
-- в таблицы sluch, usl добавлены ограничения на уникальность
-- по столбцам id_sluch, id_usl соотвественно
-- хотя можно было бы использовать в качестве первичных ключей
-- id_sluch, id_usl
create table test.schet (
id 			integer, 
code_mo 	varchar(6)	not null,
year 		numeric(4) 	not null,
month 		numeric(2) 	not null,
plat		varchar(5),
constraint schet_id_pk primary key (id),
constraint shet_year_ch check (year >= 1000 and year <= 9999),
constraint shet_month_ch check (month >=1 and month <= 12)
);

create table test.sluch (
id			integer 	not null,
id_sluch	varchar(36)	not null,
pr_nov		numeric(1) 	not null,
vidpom		numeric(2) 	not null,
moddate		timestamp 	not null,
id_schet	integer 	not null,
constraint sluch_id_pk primary key (id),
constraint sluch_id_sluch_uk unique (id_sluch),
constraint sluch_schet_fk foreign key (id_schet)
	references test.schet(id) on delete cascade,
constraint sluch_pr_nov_ch check (pr_nov in (0,1))
);

create table test.usl (
id			integer,
id_usl		varchar(36)	not null,
code_usl	varchar(16)	not null,
prvs		numeric(9) 	not null,
dateusl		date 		not null,
id_sluch	integer 	not null,
constraint usl_id_usl_pk primary key (id),
constraint sluch_id_usl_uk unique (id_usl),
constraint usl_sluch_fk foreign key (id_sluch)
	references test.sluch (id) on delete cascade
);

-- таблица загрузки xml данных
create table test.xmldata (data text);

-- выполняется скрипт для загрузки xml
\ir 'download_xml.sql'

-- для заполнения таблиц данными из xml
-- выполняется подсчёт узлов с именами SCHET, SLUCH, USL
-- во вложенных циклах перебираются данные из xml и вставляются в таблицы
create or replace procedure test.parse_xml()
as $$
declare
xml_true 	boolean := true;
path_true	boolean := false;
text_data	text := (select t.data from test.xmldata t);
xml_data 	xml;
ct_schet 	integer := 0;
ct_sluch 	integer := 0;
ct_usl		integer := 0;
sc_curr_id		integer := 0;
sl_curr_id		integer := 0;
u_curr_id		integer := 0;
begin
	-- загрузка xml в переменную чтобы не обращаться к таблице
	xml_data := (select xmlparse(document text_data));
	-- проверка корректности xml
	xml_true := (select xml_is_well_formed_document(text_data));
	-- проверка существования узла SCHET
	path_true := (select xpath_exists('//SCHET',xml_data));

	if xml_true and path_true
	then
	-- подсчёт кол-ва узлов с именем SCHET
		ct_schet := (select (xpath('count(//SCHET)', xml_data)::text[])[1]::integer);
	else
		return;
	end if;

	-- перебор узлов SCHET
	for i in 1..ct_schet
	loop
		sc_curr_id := nextval('test.ids_seq');
		
	-- заполнение таблицы test.schet
		insert into test.schet(id, code_mo, year, month, plat)
		select sc_curr_id as id,
			sc.code_mo,
			sc.yr,
			sc.mh,
			sc.plat
		from test.xmldata t 
		, xmltable (
			format('//SCHET[%s]', i)
			passing xmlparse(document t.data)
			columns 
			code_mo text path 'CODE_MO'
			,yr int path 'YEAR'
			,mh int path 'MONTH'
			,plat int path 'PLAT'
		) sc;
		
		-- подсчёт кол-ва узлов с именем SLUCH для текущего узла SCHET
		ct_sluch := (select (xpath(format('count(//SCHET[%s]/SLUCH)',i), xml_data)::text[])[1]::integer);
		
		-- перебор узлов SLUCH и заполнение таблицы test.sluch
		for s in 1..ct_sluch
		loop
			sl_curr_id := nextval('test.ids_seq');
						
			insert into test.sluch (id, id_sluch, pr_nov, vidpom, moddate, id_schet)
			select sl_curr_id,
				sl.id_sluch,
				sl.pr_nov,
				sl.vidpom,
				sl.moddate,
				sc_curr_id
			from test.xmldata t 
			, xmltable (
				format('//SCHET[%s]/SLUCH[%s]', i, s)
				passing xmlparse(document t.data)
				columns 
				id_sluch text path 'ID_SLUCH'
				,pr_nov int path 'PR_NOV'
				,vidpom int path 'VIDPOM'
				,moddate date path 'MODDATE'
			) sl; 
		
			ct_usl := (select (xpath(format('count(//SCHET[%s]/SLUCH[%s]/USL)', i, s), xml_data)::text[])[1]::integer);
			
			-- перебор узлов USL и заполнение таблицы test.usl
			for u in 1..ct_usl
			loop
				u_curr_id := nextval('test.ids_seq');
			
				insert into test.usl(id, id_usl, code_usl, prvs, dateusl, id_sluch)
				select u_curr_id,
					ux.id_usl,
					ux.code_usl,
					ux.prvs,
					ux.date_usl,
					sl_curr_id
				from test.xmldata t 
				, xmltable (
					format('//SCHET[%s]/SLUCH[%s]/USL[%s]', i, s, u)
					passing xmlparse(document t.data)
					columns 
					id_usl text path 'ID_USL'
					,code_usl text path 'CODE_USL'
					,prvs int path 'PRVS'
					,date_usl date path 'DATEUSL'
				) ux; 
				
			end loop;
			
		end loop;

--	commit;
	end loop;

end; 
$$ language plpgsql;

call test.parse_xml();
commit;

select * from test.schet;
select * from test.sluch;
select * from test.usl;



